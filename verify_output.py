import pandas as pd

# Load the output file to verify the structure
try:
    # Load the Bank Statement sheet
    df = pd.read_excel('Matched_Results_final.xlsx', sheet_name='Bank Statement')
    print('Bank Statement columns:')
    print(list(df.columns))
    print(f'\nBank Statement shape: {df.shape}')
    
    # Check if gap columns and status column exist
    gap_cols = ['Gap1', 'Gap2', 'Gap3']
    status_col = 'Status'
    
    print(f"\nGap columns present: {[col for col in gap_cols if col in df.columns]}")
    print(f"Status column present: {status_col in df.columns}")
    
    # Show a few rows to confirm the structure
    if all(col in df.columns for col in gap_cols + [status_col]):
        print(f"\nFirst 3 rows showing gap columns and status:")
        print(df[gap_cols + [status_col]].head(3))
    
    # Load the Ledger sheet as well
    df_ledger = pd.read_excel('Matched_Results_final.xlsx', sheet_name='Ledger')
    print(f'\nLedger columns:')
    print(list(df_ledger.columns))
    print(f'Ledger shape: {df_ledger.shape}')
    
    print(f"\nLedger gap columns present: {[col for col in gap_cols if col in df_ledger.columns]}")
    print(f"Ledger status column present: {status_col in df_ledger.columns}")
    
    if all(col in df_ledger.columns for col in gap_cols + [status_col]):
        print(f"\nFirst 3 rows showing gap columns and status for Ledger:")
        print(df_ledger[gap_cols + [status_col]].head(3))
        
except Exception as e:
    print(f"Error reading the file: {e}")