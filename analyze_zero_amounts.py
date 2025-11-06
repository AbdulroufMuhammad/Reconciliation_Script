import pandas as pd

# Let's check specifically for rows with valid dates but zero amounts
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
    
    # Parse the dates and amounts
    df_data['parsed_date'] = pd.to_datetime(df_data[date_col].astype(str), errors='coerce')
    df_data['parsed_credit'] = pd.to_numeric(df_data[credit_col].astype(str).str.replace(',', ''), errors='coerce')
    df_data['parsed_debit'] = pd.to_numeric(df_data[debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Check all combinations
    valid_date = df_data['parsed_date'].notna()
    credit_zero = (df_data['parsed_credit'] == 0) | df_data['parsed_credit'].isna()
    debit_zero = (df_data['parsed_debit'] == 0) | df_data['parsed_debit'].isna()
    credit_nonzero = df_data['parsed_credit'].notna() & (df_data['parsed_credit'] != 0)
    debit_nonzero = df_data['parsed_debit'].notna() & (df_data['parsed_debit'] != 0)
    
    # Cases:
    # 1. Valid date + both amounts zero/missing
    both_zero = valid_date & credit_zero & debit_zero
    print(f"Rows with valid date but both credit and debit are zero/missing: {both_zero.sum()}")
    if both_zero.sum() > 0:
        print("These rows:")
        print(df_data[both_zero])
    
    # 2. Valid date + credit zero/missing + debit non-zero
    credit_zero_debit_nonzero = valid_date & credit_zero & debit_nonzero
    print(f"Rows with valid date + zero/missing credit + non-zero debit: {credit_zero_debit_nonzero.sum()}")
    
    # 3. Valid date + debit zero/missing + credit non-zero
    debit_zero_credit_nonzero = valid_date & debit_zero & credit_nonzero
    print(f"Rows with valid date + zero/missing debit + non-zero credit: {debit_zero_credit_nonzero.sum()}")
    
    # 4. Valid date + both credit and debit non-zero
    both_nonzero = valid_date & credit_nonzero & debit_nonzero
    print(f"Rows with valid date + both credit and debit non-zero: {both_nonzero.sum()}")
    
    # The actual transactions should be rows with valid date and at least one non-zero amount
    actual_transactions = valid_date & (credit_nonzero | debit_nonzero)
    print(f"Actual transactions (valid date + at least one non-zero amount): {actual_transactions.sum()}")
    
    # Maybe the expectation is for "substantive" transactions only?
    # Let me also check if there are other criteria like minimum amount
    print(f"\nExamining amount ranges:")
    credit_values = df_data[df_data['parsed_credit'].notna()]['parsed_credit']
    if not credit_values.empty:
        print(f"Credit amounts - Min: {credit_values.min()}, Max: {credit_values.max()}, Mean: {credit_values.mean():.2f}")
        
    debit_values = df_data[df_data['parsed_debit'].notna()]['parsed_debit']
    if not debit_values.empty:
        print(f"Debit amounts - Min: {debit_values.min()}, Max: {debit_values.max()}, Mean: {debit_values.mean():.2f}")
    
    # Check if maybe very small amounts are to be excluded
    small_credit = (df_data['parsed_credit'].notna()) & (df_data['parsed_credit'] > 0) & (df_data['parsed_credit'] < 1)
    small_debit = (df_data['parsed_debit'].notna()) & (df_data['parsed_debit'] > 0) & (df_data['parsed_debit'] < 1)
    print(f"Credit amounts > 0 but < 1: {small_credit.sum()}")
    print(f"Debit amounts > 0 but < 1: {small_debit.sum()}")
    
    # The most likely explanation: you're looking for rows that have non-zero credits specifically
    # This would be 573 rows, which is very close to your expected 574
    print(f"\nThe closest match to your expected count of 574:")
    print(f"- Rows with non-zero credits: {credit_nonzero.sum()} (573)")
    print(f"- Rows with non-zero debits: {debit_nonzero.sum()} (49)")
    print(f"- Total actual transactions: {actual_transactions.sum()} (622)")
    
    # Let's see if there are exactly 622-574=48 rows that might be filtered out
    # to get to 574 from 622
    difference = 622 - 574  # = 48
    print(f"You're looking for {difference} fewer records than the current 622")
    print(f"There are {debit_nonzero.sum()} = 49 debit-only records")
    print(f"Is it possible you want only credit transactions? (which would be 573, very close to 574)")
    
    # Maybe you want credit transactions only?
    credit_only_transactions = valid_date & credit_nonzero & (debit_zero | df_data['parsed_debit'].isna())
    print(f"\nCredit transactions only (non-zero credit, zero/missing debit): {credit_only_transactions.sum()}")
    
    # Let's also check if there's a special row that might account for the difference
    print(f"\nLet's check around index 573 to see if there's a special row:")
    for idx in [572, 573, 574, 575]:
        if idx < len(df_data):
            row = df_data.iloc[idx]
            print(f"Row {idx}: Credit={row['parsed_credit']}, Debit={row['parsed_debit']}, Date={row['parsed_date']}")