import pandas as pd
import os

# Let's inspect the actual bank statement file being used
file_path = 'DATA/newdata/bankstatement_2.xlsx'
print(f"Inspecting {file_path}...")

# Load the file without considering headers
df = pd.read_excel(file_path, header=None)

print(f"Shape: {df.shape}")
print(f"Total rows: {len(df)}")

print("\nFirst 20 rows:")
print(df.head(20))

print(f"\nLast 10 rows:")
print(df.tail(10))

# Check for empty rows
empty_rows = df.isnull().all(axis=1).sum()
print(f"\nEmpty rows: {empty_rows}")

# Look for the actual header row and transaction data
print("\nLooking for headers...")
header_row = None
for i in range(min(20, len(df))):  # Check first 20 rows for headers
    row_values = df.iloc[i].astype(str)
    if any('value date' in val.lower() or 'date' in val.lower() and 'value' in val.lower() for val in row_values) and \
       any('credit' in val.lower() or 'debit' in val.lower() for val in row_values):
        header_row = i
        print(f"Found potential header at row {i}: {row_values.tolist()}")
        break

if header_row is not None:
    print(f"\nData after header row {header_row}:")
    data_after_header = df.iloc[header_row+1:]
    
    # Filter out completely empty rows
    data_after_header = data_after_header.dropna(how='all')
    print(f"Shape after removing header and empty rows: {data_after_header.shape}")
    
    # Look for actual transaction data
    print(f"\nFirst 10 rows after header and empty row removal:")
    print(data_after_header.head(10))
else:
    print("No header row found with expected column names")