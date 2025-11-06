import pandas as pd
import numpy as np

# Let's analyze the data more thoroughly to understand what should be counted as transactions
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
    
    # Look for columns that match the expected names
    date_col = None
    credit_col = None
    debit_col = None
    balance_col = None
    
    for col in df_data.columns:
        if 'value date' in str(col).lower() or 'trans date' in str(col).lower() or 'date' in str(col).lower():
            date_col = col
        elif 'credit' in str(col).lower():
            credit_col = col
        elif 'debit' in str(col).lower():
            debit_col = col
        elif 'balance' in str(col).lower():
            balance_col = col
    
    print(f"Date column: {date_col}")
    print(f"Credit column: {credit_col}")
    print(f"Debit column: {debit_col}")
    print(f"Balance column: {balance_col}")
    
    # Parse the dates and amounts
    df_data['parsed_date'] = pd.to_datetime(df_data[date_col].astype(str), errors='coerce')
    df_data['parsed_credit'] = pd.to_numeric(df_data[credit_col].astype(str).str.replace(',', ''), errors='coerce')
    df_data['parsed_debit'] = pd.to_numeric(df_data[debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Check which rows have valid dates
    valid_date_mask = df_data['parsed_date'].notna()
    print(f"Rows with valid dates: {valid_date_mask.sum()}")
    
    # Check which rows have valid amounts
    valid_credit_mask = df_data['parsed_credit'].notna() & (df_data['parsed_credit'] != 0)
    valid_debit_mask = df_data['parsed_debit'].notna() & (df_data['parsed_debit'] != 0)
    print(f"Rows with valid non-zero credits: {valid_credit_mask.sum()}")
    print(f"Rows with valid non-zero debits: {valid_debit_mask.sum()}")
    
    # Combine: either credit or debit should be non-zero (transactions)
    valid_amount_mask = valid_credit_mask | valid_debit_mask
    print(f"Rows with valid non-zero credits OR debits: {valid_amount_mask.sum()}")
    
    # Combine date and amount validity
    valid_transaction_mask = valid_date_mask & valid_amount_mask
    print(f"Rows with valid dates AND non-zero amounts: {valid_transaction_mask.sum()}")
    
    # Let's see what we have after filtering to actual transactions
    df_transactions = df_data[valid_transaction_mask].copy()
    print(f"\nFinal transaction data shape: {df_transactions.shape}")
    
    # Look at some of the records that might have been excluded to understand the difference
    excluded_mask = ~valid_transaction_mask
    excluded_records = df_data[excluded_mask]
    print(f"\nExcluded records shape: {excluded_records.shape}")
    print("Some excluded records (if any):")
    if len(excluded_records) > 0:
        print(excluded_records.head(10))
        
    print(f"\nTransaction records with valid data:")
    print(df_transactions.head(10))
    
    # Also check if there are records with both credit and debit as zero, which might be excluded
    zero_amount_mask = (df_data['parsed_credit'] == 0) | (df_data['parsed_debit'] == 0)
    both_zero_mask = (df_data['parsed_credit'] == 0) & (df_data['parsed_debit'] == 0)
    print(f"\nRecords with both credit and debit as zero: {both_zero_mask.sum()}")
    
    # Let's also see if there are records where credit is NaN but debit has value, or vice versa
    credit_nan_debit_valid = df_data['parsed_credit'].isna() & df_data['parsed_debit'].notna() & (df_data['parsed_debit'] != 0)
    debit_nan_credit_valid = df_data['parsed_debit'].isna() & df_data['parsed_credit'].notna() & (df_data['parsed_credit'] != 0)
    print(f"Records with NaN credit but valid non-zero debit: {credit_nan_debit_valid.sum()}")
    print(f"Records with NaN debit but valid non-zero credit: {debit_nan_credit_valid.sum()}")
    
    # Also check if there are records that have both credit and debit values (might be special cases)
    both_nonzero = valid_credit_mask & valid_debit_mask  # Records with both non-zero
    print(f"Records with both credit and debit non-zero: {both_nonzero.sum()}")
    
    print(f"\nLooking more specifically at the range around 574 records:")
    # If we're expecting 574 records, let's look at the data around that index
    if len(df_data) > 574:
        print("Data around index 573-576 (expected count):")
        print(df_data.iloc[570:580])
    
    # Check for rows that might contain summary information but slipped through
    summary_keywords = ['total', 'balance', 'summary', 'closing', 'opening', 'grand total', 'sub total', 'overall']
    potential_summary_rows = []
    for idx, row in df_data.iterrows():
        # Convert the entire row to string for keyword search
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
        row_str_lower = row_str.lower()
        
        # Check if any summary keyword exists in the row
        if any(keyword in row_str_lower for keyword in summary_keywords):
            potential_summary_rows.append(idx)
    
    print(f"\nFound potential summary rows: {len(potential_summary_rows)} at indices: {potential_summary_rows[:10]}...")  # Show first 10