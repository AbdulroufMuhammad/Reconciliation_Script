import pandas as pd

# Let's inspect the sample bank statement file to see its raw structure
print("Inspecting sample_bank_statement.xlsx...")
df = pd.read_excel('sample_bank_statement.xlsx', header=None)

print(f"Shape: {df.shape}")
print("\nFirst 10 rows:")
print(df.head(10))

print(f"\nLast 10 rows:")
print(df.tail(10))

print(f"\nTotal rows: {len(df)}")

# Let's also check if there are empty rows
empty_rows = df.isnull().all(axis=1).sum()
print(f"\nEmpty rows: {empty_rows}")

# Check the middle part of the file to understand structure
print(f"\nRows around the middle (from index {len(df)//2 - 5} to {len(df)//2 + 5}):")
print(df.iloc[len(df)//2 - 5:len(df)//2 + 5])