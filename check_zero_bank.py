import pandas as pd

# Let's specifically analyze the bank statement to see how many zero-value records exist
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
    
    # Parse the amounts
    parsed_credit = pd.to_numeric(df_data[credit_col].astype(str).str.replace(',', ''), errors='coerce')
    parsed_debit = pd.to_numeric(df_data[debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Count different types of records
    credit_nonzero = (parsed_credit.notna()) & (parsed_credit != 0)
    debit_nonzero = (parsed_debit.notna()) & (parsed_debit != 0)
    both_zero = (~credit_nonzero) & (~debit_nonzero)  # Both credit and debit are zero or NaN
    
    print(f"Bank Statement Analysis:")
    print(f"Total records after filtering: {len(df_data)}")
    print(f"Records with non-zero credit: {credit_nonzero.sum()}")
    print(f"Records with non-zero debit: {debit_nonzero.sum()}")
    print(f"Records with both credit and debit zero: {both_zero.sum()}")
    print(f"Records with at least one non-zero amount: {(credit_nonzero | debit_nonzero).sum()}")
    
    # The expected result after our filtering should be records with at least one non-zero amount
    expected_count = (credit_nonzero | debit_nonzero).sum()
    print(f"Expected count after filtering zero amounts: {expected_count}")
    
    # Check if there are any records where both credit and debit are zero
    if both_zero.sum() > 0:
        print("Sample of zero-amount records:")
        zero_records = df_data[both_zero]
        print(zero_records.head(10))