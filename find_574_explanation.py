import pandas as pd

# Let's check if there are any other criteria for what constitutes valid records
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
    print(f"Shape after removing empty rows: {df_data.shape}")
    
    # Look for the columns
    date_col = None
    credit_col = None
    debit_col = None
    
    for col in df_data.columns:
        if 'value date' in str(col).lower() or 'trans date' in col.lower():
            date_col = col
        elif 'credit' in col.lower():
            credit_col = col
        elif 'debit' in col.lower():
            debit_col = col
    
    print(f"Date column: {date_col}, Credit column: {credit_col}, Debit column: {debit_col}")
    
    # Parse the dates and amounts more thoroughly
    df_data['parsed_date'] = pd.to_datetime(df_data[date_col].astype(str), errors='coerce')
    df_data['parsed_credit'] = pd.to_numeric(df_data[credit_col].astype(str).str.replace(',', ''), errors='coerce')
    df_data['parsed_debit'] = pd.to_numeric(df_data[debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Check for valid dates
    valid_date_mask = df_data['parsed_date'].notna()
    print(f"Rows with valid dates: {valid_date_mask.sum()}")
    
    # Check for valid numeric amounts
    valid_credit_mask = df_data['parsed_credit'].notna()
    valid_debit_mask = df_data['parsed_debit'].notna()
    print(f"Rows with valid credit values: {valid_credit_mask.sum()}")
    print(f"Rows with valid debit values: {valid_debit_mask.sum()}")
    
    # Look specifically for actual transactions (non-zero amounts)
    nonzero_credit_mask = (df_data['parsed_credit'].notna()) & (df_data['parsed_credit'] != 0)
    nonzero_debit_mask = (df_data['parsed_debit'].notna()) & (df_data['parsed_debit'] != 0)
    print(f"Rows with non-zero credit: {nonzero_credit_mask.sum()}")
    print(f"Rows with non-zero debit: {nonzero_debit_mask.sum()}")
    
    # Transactions that have either non-zero credit or non-zero debit
    transaction_mask = nonzero_credit_mask | nonzero_debit_mask
    print(f"Actual transaction records (non-zero credit or debit): {transaction_mask.sum()}")
    
    # If we're looking for 574 records, there might be two possible explanations:
    # 1. Your expected count includes some rows I'm excluding (not likely since 574 < 622)
    # 2. You expect only non-zero amounts, which would be 573+49 = 622, still not 574
    # 3. Perhaps some range of the data is what you consider 'transactions'
    
    # Let me check if there are any other columns or metadata that might be relevant
    print(f"\nColumn names: {list(df_with_headers.columns)}")
    
    # Maybe you're only counting credit transactions? (573 - very close to 574)
    print(f"\nIf counting only non-zero credit entries: {nonzero_credit_mask.sum()}")
    
    # Or maybe there's a specific date range?
    print(f"\nDate range in data: {df_data['parsed_date'].min()} to {df_data['parsed_date'].max()}")
    
    # Let's also check for any rows that might be duplicates or special entries
    print(f"\nChecking for potential duplicate rows based on date and amount:")
    df_data['date_amount'] = df_data['parsed_date'].dt.strftime('%Y-%m-%d') + '_' + (df_data['parsed_credit'].fillna(0) + df_data['parsed_debit'].fillna(0)).astype(str)
    duplicates = df_data['date_amount'].duplicated().sum()
    print(f"Potential duplicate entries (based on date and combined amount): {duplicates}")
    
    # Let's also see if row 574 (index 573) has any special significance
    if len(df_data) > 574:
        print(f"\nLooking at the row around your expected count (index 573-574):")
        print(df_data.iloc[572:577])  # Show 5 rows around the expected 574
        
        # Maybe there's a cutoff point that's not just date/amount validity
        # Check for specific transaction types or patterns
        
    # Let's try to find the most likely explanation for 574
    print(f"\nAnalyzing different possible counts:")
    print(f"1. All data rows after header: {len(df_data)}")
    print(f"2. Rows with valid dates: {valid_date_mask.sum()}")
    print(f"3. Rows with non-zero credits: {nonzero_credit_mask.sum()}")
    print(f"4. Rows with non-zero debits: {nonzero_debit_mask.sum()}")
    print(f"5. Rows with non-zero credits or debits: {transaction_mask.sum()}")
    
    # Maybe there's an ending header or summary section?
    # Check if the last few rows have different characteristics
    print(f"\nLast 10 rows to see if there are ending summaries:")
    last_10 = df_data.tail(10)
    for idx, row in last_10.iterrows():
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
        print(f"Row {idx}: {row_str[:80]}...")
    
    # Maybe there are special transaction types to exclude?
    # Check if there's a description column that might indicate summary entries
    for col in df_with_headers.columns:
        if 'remark' in str(col).lower() or 'description' in str(col).lower() or 'narration' in str(col).lower():
            print(f"\nAnalyzing {col} column for patterns...")
            # Check for any special transaction types
            unique_remarks = df_data[col].astype(str).str.lower().unique()
            summary_indicators = [remark for remark in unique_remarks 
                                if any(keyword in remark for keyword in 
                                      ['total', 'summary', 'balance', 'closing', 'opening', 'overall'])]
            print(f"Potential summary indicators in {col}: {summary_indicators[:10]}")