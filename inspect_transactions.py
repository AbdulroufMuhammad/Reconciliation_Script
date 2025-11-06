import pandas as pd
import numpy as np

# Let's inspect the bank statement file more thoroughly to understand the structure
file_path = 'DATA/newdata/bankstatement_2.xlsx'
df = pd.read_excel(file_path, header=None)

print(f"Total rows in file: {len(df)}")

# Find the header row
header_row = None
for i in range(min(20, len(df))):
    row_values = df.iloc[i].astype(str)
    if any('value date' in val.lower() for val in row_values) and \
       any('credit' in val.lower() or 'debit' in val.lower() for val in row_values):
        header_row = i
        break

if header_row is not None:
    print(f"Header found at row: {header_row}")
    # Get the data after header
    df_with_headers = df.iloc[header_row:].copy()
    df_with_headers.columns = df_with_headers.iloc[0]  # Set first row as header
    df_data = df_with_headers.iloc[1:].reset_index(drop=True)  # Get data after header
    
    print(f"Shape after setting headers: {df_data.shape}")
    
    # Filter out completely empty rows
    df_data = df_data.dropna(how='all')
    print(f"Shape after removing empty rows: {df_data.shape}")
    
    # Now let's look for rows that might not be transactions
    # Look for potential summary rows, totals, etc.
    print("\nLooking for possible non-transaction rows...")
    print("First 10 rows:")
    print(df_data.head(10))
    
    print("\nLast 20 rows (to see if there are summaries or totals at the end):")
    print(df_data.tail(20))
    
    # Check for rows that might contain summary information like "Total", "Balance", etc.
    possible_summary_rows = []
    for idx, row in df_data.iterrows():
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
        if any(keyword in row_str.lower() for keyword in ['total', 'balance', 'summary', 'closing', 'opening']):
            possible_summary_rows.append(idx)
    
    if possible_summary_rows:
        print(f"\nFound possible summary rows at indices: {possible_summary_rows}")
        print("These rows might need to be excluded from the transaction count")
        df_transactions = df_data.drop(possible_summary_rows)
        print(f"Shape after removing possible summary rows: {df_transactions.shape}")
    else:
        print("\nNo obvious summary rows found based on keywords")
        df_transactions = df_data
    
    # Check for rows that have valid dates and amounts
    # Look for specific date and amount columns
    date_col = None
    amount_cols = []
    
    for col in df_transactions.columns:
        if 'value date' in str(col).lower() or 'trans date' in str(col).lower() or 'date' in str(col).lower():
            date_col = col
            break
    
    for col in df_transactions.columns:
        if 'credit' in str(col).lower() or 'debit' in str(col).lower() or 'amount' in str(col).lower():
            amount_cols.append(col)
    
    print(f"\nDate column identified: {date_col}")
    print(f"Amount columns identified: {amount_cols}")
    
    if date_col:
        # Try to parse dates to see which rows have valid transaction dates
        date_series = pd.to_datetime(df_transactions[date_col].astype(str), errors='coerce')
        valid_date_mask = date_series.notna()
        print(f"Rows with valid dates: {valid_date_mask.sum()}")
        
        # Look at amount columns
        if amount_cols:
            for amount_col in amount_cols:
                amount_series = pd.to_numeric(df_transactions[amount_col].astype(str).str.replace(',', '').replace(' ', ''), errors='coerce')
                valid_amount_mask = amount_series.notna() & (amount_series != 0)  # Exclude zero amounts
                print(f"Rows with valid non-zero amounts in {amount_col}: {valid_amount_mask.sum()}")
                
                # Combine date and amount validity
                valid_transaction_mask = valid_date_mask & valid_amount_mask
                print(f"Rows with both valid dates and non-zero amounts in {amount_col}: {valid_transaction_mask.sum()}")
    
    # Let's also see if the last few rows might be summary information
    print(f"\nAnalyzing the end of the data...")
    tail_data = df_data.tail(30)  # Look at last 30 rows
    print("Last 30 rows:")
    for idx in tail_data.index:
        row = tail_data.loc[idx]
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
        print(f"Row {idx}: {row_str[:100]}...")  # Print first 100 chars