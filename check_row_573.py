import pandas as pd

# Let's look more closely at the row that was identified as a potential summary row
file_path = 'DATA/newdata/bankstatement_2.xlsx'
df = pd.read_excel(file_path, header=None)

# Find the header row
header_row = None
for i in range(min(20, len(df))):
    row_values = df.iloc[i].astype(str)
    if any('value date' in val.lower() or 'trans date' in val.lower() for val in row_values) and \
       any('credit' in val.lower() or 'debit' in val.lower() for val in row_values):
        header_row = i
        break

if header_row is not None:
    # Get the data after header
    df_with_headers = df.iloc[header_row:].copy()
    df_with_headers.columns = df_with_headers.iloc[0]  # Set first row as header
    df_data = df_with_headers.iloc[1:].reset_index(drop=True)  # Get data after header
    
    # Filter out completely empty rows
    df_data = df_data.dropna(how='all')
    
    # Look specifically at row 573 (the one that was identified as potential summary)
    row_573 = df_data.iloc[573] if len(df_data) > 573 else "Row does not exist"
    
    if isinstance(row_573, pd.Series):
        print("Row 573 content:")
        for col_name, value in row_573.items():
            print(f"  {col_name}: {value}")
        
        # Check what text would match our summary keywords
        row_str = ' '.join(str(val) for val in row_573.values if pd.notna(val))
        print(f"\nFull row as text: {row_str}")
        print(f"Looking for keywords: ['total', 'balance', 'summary', 'closing', 'opening', 'grand total', 'sub total', 'overall']")
        
        summary_keywords = ['total', 'balance', 'summary', 'closing', 'opening', 'grand total', 'sub total', 'overall']
        found_keywords = []
        for keyword in summary_keywords:
            if keyword in row_str.lower():
                found_keywords.append(keyword)
        
        if found_keywords:
            print(f"Found keywords: {found_keywords}")
        else:
            print("No summary keywords found in this row!")
    
    print(f"\nLooking at a few surrounding rows to get context:")
    start_idx = max(0, 570)
    end_idx = min(len(df_data), 580)
    print(df_data.iloc[start_idx:end_idx])
    
    # Let's also look for any other potential issues with our filtering
    print(f"\nAnalyzing the actual issue:")
    print(f"Total original data after headers: {len(df_data)}")
    
    # Check if there are rows with just balance information but no actual transactions
    balance_col_idx = None
    for i, col_name in enumerate(df_with_headers.columns):
        if 'balance' in str(col_name).lower():
            balance_col_idx = i
            break
    
    if balance_col_idx is not None:
        # Check if there are rows where all transaction-related columns are empty but balance is present
        credit_debit_cols = []
        for i, col_name in enumerate(df_with_headers.columns):
            if 'credit' in str(col_name).lower() or 'debit' in str(col_name).lower() or 'amount' in str(col_name).lower():
                credit_debit_cols.append(i)
        
        print(f"Credit/debit columns indices: {credit_debit_cols}")
        
        # Find rows where credit and debit are empty but balance exists
        for idx, row in df_data.iterrows():
            has_credit_debit = False
            for col_idx in credit_debit_cols:
                if col_idx < len(row) and pd.notna(row.iloc[col_idx]) and str(row.iloc[col_idx]).strip() != '' and str(row.iloc[col_idx]).strip() != '0':
                    has_credit_debit = True
                    break
            
            balance_val = row.iloc[balance_col_idx] if balance_col_idx < len(row) else None
            has_balance = pd.notna(balance_val) and str(balance_val).strip() != ''
            
            if not has_credit_debit and has_balance:
                print(f"Row {idx}: Has balance ({balance_val}) but no credit/debit amounts - could be a balance row")
    
    # Let's also look for what your expected 574 might be based on the data
    print(f"\nAnalyzing why you might expect 574 records:")
    print(f"Number of rows with non-zero credits: 573 (as per our analysis)")
    print(f"Number of rows with non-zero debits: 49 (as per our analysis)")
    print(f"Total unique transaction rows: 622 (all rows have either credit or debit or both)")
    
    # Maybe there are some rows that have both zero values or are just headers?
    # Let's look for rows that might be different from the rest
    date_col_idx = None
    for i, col_name in enumerate(df_with_headers.columns):
        if 'date' in str(col_name).lower() and 'value' in str(col_name).lower():
            date_col_idx = i
            break
    
    if date_col_idx is not None:
        print(f"\nLooking for rows with suspicious date values...")
        for idx, row in df_data.iterrows():
            date_val = row.iloc[date_col_idx] if date_col_idx < len(row) else None
            if pd.notna(date_val):
                try:
                    # Check if this looks like an actual date
                    pd.to_datetime(str(date_val))
                except:
                    print(f"Row {idx}: Has suspicious date value: {date_val}")