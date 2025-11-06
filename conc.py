import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def find_value_date_and_amount_columns(df, file_type):
    """
    Find Value Date and Credit/Debit columns in the dataframe.
    """
    date_col = None
    amount_col = None
    
    # Look for exact "Value Date" column
    for col in df.columns:
        if str(col).strip().lower() == 'value date':
            date_col = col
            break
    
    # If not found, check for common variations
    if date_col is None:
        for col in df.columns:
            if str(col).strip().lower().replace(' ', '').replace('_', '') in ['valuedate', 'value_date', 'valuedate']:
                date_col = col
                break
    
    # Find Credit (for bank) or Debit (for ledger) column
    if file_type == "bank":
        for col in df.columns:
            if str(col).strip().lower() == 'credit':
                amount_col = col
                break
        # If not found, check for variations
        if amount_col is None:
            for col in df.columns:
                if str(col).strip().lower().replace(' ', '').replace('_', '') in ['credit', 'cr', 'credits']:
                    amount_col = col
                    break
    elif file_type == "ledger":
        for col in df.columns:
            if str(col).strip().lower() == 'debit':
                amount_col = col
                break
        # If not found, check for variations
        if amount_col is None:
            for col in df.columns:
                if str(col).strip().lower().replace(' ', '').replace('_', '') in ['debit', 'dr', 'debits', 'withdrawal']:
                    amount_col = col
                    break
    
    return date_col, amount_col

def reconcile_bank_to_ledger(bank_file, ledger_file, output_file):
    """
    Reconcile bank statement against ledger based on Value Date and Amounts.
    """
    print("="*60)
    print("BANK RECONCILIATION SCRIPT")
    print("="*60)
    print("BASE RECORD: Bank Statement")
    print("MATCHING: Bank [Value Date + Amount(Credit)] with Ledger [Value Date + Amount(Debit)]")
    print("="*60)
    
    # Load the files
    print(f"Loading Bank Statement: {bank_file}")
    if bank_file.lower().endswith('.xlsx') or bank_file.lower().endswith('.xls'):
        bank_df = pd.read_excel(bank_file, header=None)
    else:
        bank_df = pd.read_csv(bank_file, header=None)
    
    print(f"Loading Ledger: {ledger_file}")
    if ledger_file.lower().endswith('.xlsx') or ledger_file.lower().endswith('.xls'):
        ledger_df = pd.read_excel(ledger_file, header=None)
    else:
        ledger_df = pd.read_csv(ledger_file, header=None)
    
    print(f"Bank Statement shape: {bank_df.shape}")
    print(f"Ledger shape: {ledger_df.shape}")
    
    # Find header rows by looking for "Value Date", "Credit", "Debit" keywords
    bank_header_row = None
    for i in range(min(20, len(bank_df))):  # Check first 20 rows for headers
        row_values = bank_df.iloc[i].astype(str)
        if any('value date' in val.lower() for val in row_values) and \
           any('credit' in val.lower() for val in row_values):
            bank_header_row = i
            break
    
    ledger_header_row = None
    for i in range(min(20, len(ledger_df))):  # Check first 20 rows for headers
        row_values = ledger_df.iloc[i].astype(str)
        if any('value date' in val.lower() for val in row_values) and \
           any('debit' in val.lower() for val in row_values):
            ledger_header_row = i
            break

    # If exact headers not found, try to find any date/credit or date/debit combination
    if bank_header_row is None:
        for i in range(min(20, len(bank_df))):
            row_values = bank_df.iloc[i].astype(str)
            has_date = any('date' in val.lower() and 'value' in val.lower() for val in row_values)
            has_credit = any('credit' in val.lower() for val in row_values)
            if has_date and has_credit:
                bank_header_row = i
                break
    
    if ledger_header_row is None:
        for i in range(min(20, len(ledger_df))):
            row_values = ledger_df.iloc[i].astype(str)
            has_date = any('date' in val.lower() and 'value' in val.lower() for val in row_values)
            has_debit = any('debit' in val.lower() for val in row_values)
            if has_date and has_debit:
                ledger_header_row = i
                break

    # Set headers if found
    if bank_header_row is not None:
        print(f"Found bank header at row {bank_header_row}")
        bank_df.columns = bank_df.iloc[bank_header_row]
        bank_df = bank_df.iloc[bank_header_row+1:]
    else:
        print("No header row found for bank file, using default column names")
        # If no header found, just use default names
        bank_df.columns = [str(col) for col in bank_df.columns]
    
    if ledger_header_row is not None:
        print(f"Found ledger header at row {ledger_header_row}")
        ledger_df.columns = ledger_df.iloc[ledger_header_row]
        ledger_df = ledger_df.iloc[ledger_header_row+1:]
    else:
        print("No header row found for ledger file, using default column names")
        # If no header found, just use default names
        ledger_df.columns = [str(col) for col in ledger_df.columns]
    
    # Find the specific Value Date and Credit/Debit columns
    bank_date_col, bank_credit_col = find_value_date_and_amount_columns(bank_df, "bank")
    ledger_date_col, ledger_debit_col = find_value_date_and_amount_columns(ledger_df, "ledger")
    
    print(f"Bank Date Column: {bank_date_col}")
    print(f"Bank Credit Column: {bank_credit_col}")
    print(f"Ledger Date Column: {ledger_date_col}")
    print(f"Ledger Debit Column: {ledger_debit_col}")
    
    # Check if required columns were found
    if bank_date_col is None or bank_credit_col is None:
        print("ERROR: Could not find 'Value Date' and 'Credit' columns in bank file")
        return
    if ledger_date_col is None or ledger_debit_col is None:
        print("ERROR: Could not find 'Value Date' and 'Debit' columns in ledger file")
        return
    
    # Convert date columns to datetime
    bank_df['clean_date'] = pd.to_datetime(bank_df[bank_date_col], errors='coerce')
    ledger_df['clean_date'] = pd.to_datetime(ledger_df[ledger_date_col], errors='coerce')
    
    # Convert amount columns to numeric (these will be used internally for matching)
    # Both Credit and Debit are converted to Amount concept internally
    # Handle comma-formatted numbers in the data
    bank_df['internal_amount'] = pd.to_numeric(bank_df[bank_credit_col].astype(str).str.replace(',', ''), errors='coerce')
    ledger_df['internal_amount'] = pd.to_numeric(ledger_df[ledger_debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Prepare for matching by creating comparison keys
    # For matching, we compare date and absolute amount (converting both Credit and Debit to Amount)
    # Use a more precise way to handle floating point comparisons
    bank_df['match_date'] = bank_df['clean_date'].dt.strftime('%Y-%m-%d')
    ledger_df['match_date'] = ledger_df['clean_date'].dt.strftime('%Y-%m-%d')
    
    # Round amounts to handle floating point precision issues
    # Both Credit and Debit are converted to Amount for matching purposes
    bank_df['match_amount'] = bank_df['internal_amount'].abs().round(2)
    ledger_df['match_amount'] = ledger_df['internal_amount'].abs().round(2)
    
    # More flexible approach: Convert to string and remove commas that might be in the data
    bank_df['match_date'] = bank_df['match_date'].astype(str)
    ledger_df['match_date'] = ledger_df['match_date'].astype(str)
    
    # Perform the matching using a more efficient pandas merge
    print("Performing reconciliation...")
    
    # Initialize status columns
    bank_df['Status'] = 'Unmatched with Ledger'
    ledger_df['Status'] = 'Unmatched with Bank'
    
    # Add original indices for tracking
    bank_df['original_index'] = bank_df.index
    ledger_df['original_index'] = ledger_df.index
    
    # Create temporary DataFrames for matching
    bank_temp = bank_df[['match_date', 'match_amount', 'original_index']].copy()
    ledger_temp = ledger_df[['match_date', 'match_amount', 'original_index']].copy()
    
    # Perform the merge to find ALL possible matches first
    all_matches = pd.merge(
        bank_temp,
        ledger_temp,
        on=['match_date', 'match_amount'],
        how='inner',
        suffixes=('_bank', '_ledger')
    )
    
    # Now implement one-to-one matching to avoid duplicate matches
    matched_bank_indices = set()
    matched_ledger_indices = set()
    
    # Process all possible matches and ensure one-to-one matching
    for _, match_row in all_matches.iterrows():
        bank_idx = match_row['original_index_bank']
        ledger_idx = match_row['original_index_ledger']
        
        # Only mark as matched if both records haven't been matched yet
        if bank_idx not in matched_bank_indices and ledger_idx not in matched_ledger_indices:
            matched_bank_indices.add(bank_idx)
            matched_ledger_indices.add(ledger_idx)
    
    # Mark matched records in original DataFrames
    bank_matched_indices_list = list(matched_bank_indices)
    ledger_matched_indices_list = list(matched_ledger_indices)
    
    # Update status for matched records
    if bank_matched_indices_list:  # Only update if there are matches
        bank_df.loc[bank_matched_indices_list, 'Status'] = 'Matched with Ledger'
    if ledger_matched_indices_list:  # Only update if there are matches
        ledger_df.loc[ledger_matched_indices_list, 'Status'] = 'Matched with Bank'
    
    # Clean up temporary columns
    bank_df = bank_df.drop('original_index', axis=1)
    ledger_df = ledger_df.drop('original_index', axis=1)

    # Calculate summary
    total_bank_records = len(bank_df)
    matched_bank_records = len(bank_matched_indices_list)
    unmatched_bank_records = total_bank_records - matched_bank_records
    
    total_ledger_records = len(ledger_df)
    matched_ledger_records = len(ledger_matched_indices_list)
    unmatched_ledger_records = total_ledger_records - matched_ledger_records
    
    match_rate = (matched_bank_records / min(total_bank_records, total_ledger_records) * 100) if min(total_bank_records, total_ledger_records) > 0 else 0
    
    print("\n=== Reconciliation Summary ===")
    print(f"BANK STATEMENT (BASE):")
    print(f"  - Total records: {total_bank_records}")
    print(f"  - Matched with Ledger: {matched_bank_records}")
    print(f"  - Unmatched with Ledger: {unmatched_bank_records}")
    
    print(f"\nLEDGER:")
    print(f"  - Total records: {total_ledger_records}")
    print(f"  - Matched with Bank: {matched_ledger_records}")
    print(f"  - Unmatched with Bank: {unmatched_ledger_records}")
    
    print(f"\nMatch Rate: {match_rate:.2f}%")
    
    # Create output workbook with all required sheets
    print(f"\nSaving results to: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = {
            'Metric': [
                'Total Bank Statement records',
                'Bank records matched with Ledger',
                'Bank records unmatched with Ledger',
                'Total Ledger records',
                'Ledger records matched with Bank',
                'Ledger records unmatched with Bank',
                'Match rate percentage'
            ],
            'Value': [
                total_bank_records,
                matched_bank_records,
                unmatched_bank_records,
                total_ledger_records,
                matched_ledger_records,
                unmatched_ledger_records,
                f"{match_rate:.2f}%"
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Bank Statement sheet with status (keeping original structure)
        cols_to_save = [col for col in bank_df.columns if col not in ['clean_date', 'internal_amount', 'match_key']]
        bank_df[cols_to_save].to_excel(writer, sheet_name='Bank Statement', index=False)
        
        # Bank - Matched sheet
        bank_matched = bank_df[bank_df['Status'] == 'Matched with Ledger']
        bank_matched[cols_to_save].to_excel(writer, sheet_name='Bank - Matched', index=False)
        
        # Bank - Unmatched sheet
        bank_unmatched = bank_df[bank_df['Status'] == 'Unmatched with Ledger']
        bank_unmatched[cols_to_save].to_excel(writer, sheet_name='Bank - Unmatched', index=False)
        
        # Ledger sheet with status (keeping original structure)
        cols_to_save_ledger = [col for col in ledger_df.columns if col not in ['clean_date', 'internal_amount', 'match_key']]
        ledger_df[cols_to_save_ledger].to_excel(writer, sheet_name='Ledger', index=False)
        
        # Ledger - Matched sheet
        ledger_matched = ledger_df[ledger_df['Status'] == 'Matched with Bank']
        ledger_matched[cols_to_save_ledger].to_excel(writer, sheet_name='Ledger - Matched', index=False)
        
        # Ledger - Unmatched sheet
        ledger_unmatched = ledger_df[ledger_df['Status'] == 'Unmatched with Bank']
        ledger_unmatched[cols_to_save_ledger].to_excel(writer, sheet_name='Ledger - Unmatched', index=False)
    
    print("Results saved successfully!")
    print("\nOutput file contains:")
    print("  1. Summary - Overview of reconciliation")
    print("  2. Bank Statement - Full data with status")
    print("  3. Bank - Matched - Only matched records")
    print("  4. Bank - Unmatched - Only unmatched records")
    print("  5. Ledger - Full data with status")
    print("  6. Ledger - Matched - Only matched records")
    print("  7. Ledger - Unmatched - Only unmatched records")
    
    print("\n" + "="*60)
    print("RECONCILIATION COMPLETE!")
    print("="*60)

def main():
    """
    Main function that reads configuration from .env file.
    """
    # Read file paths from environment variables
    BANK_FILE = os.getenv('BANK_STATEMENT_FILE_PATH', 'sample_bank_statement.xlsx')
    LEDGER_FILE = os.getenv('LEDGER_FILE_PATH', 'sample_ledger.xlsx')
    OUTPUT_FILE = os.getenv('OUTPUT_FILE_PATH', 'Matched_Results_final.xlsx')
    
    # Run the reconciliation
    reconcile_bank_to_ledger(BANK_FILE, LEDGER_FILE, OUTPUT_FILE)

if __name__ == "__main__":
    main()