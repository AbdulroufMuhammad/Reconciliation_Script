import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# Explicitly load .env file
script_dir = Path(__file__).parent
env_path = script_dir / '.env'
print(f"Explicitly loading .env file from: {env_path.absolute()}")
load_dotenv(env_path, override=True)


def find_value_date_and_amount_columns(df, file_type):
    """Locate date and amount columns in the dataframe"""
    date_col, amount_col = None, None

    # Locate Value Date
    for col in df.columns:
        if str(col).strip().lower() == 'value date':
            date_col = col
            break

    if date_col is None:
        for col in df.columns:
            col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
            if col_clean in ['valuedate', 'value_date', 'date', 'transdate', 'transactiondate']:
                date_col = col
                break

    # Locate Credit/Debit column
    if file_type == "bank":
        for col in df.columns:
            if str(col).strip().lower() == 'credit':
                amount_col = col
                break
        if amount_col is None:
            for col in df.columns:
                col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
                if col_clean in ['credit', 'cr', 'credits', 'amount']:
                    amount_col = col
                    break
    elif file_type == "ledger":
        for col in df.columns:
            if str(col).strip().lower() == 'debit':
                amount_col = col
                break
        if amount_col is None:
            for col in df.columns:
                col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
                if col_clean in ['debit', 'dr', 'debits', 'withdrawal', 'amount']:
                    amount_col = col
                    break

    return date_col, amount_col


def find_actual_data_rows(df, file_type):
    """Find the header row and extract actual data"""
    header_row = None
    for i in range(min(50, len(df))):
        row_values = df.iloc[i].astype(str).str.lower()
        has_date = any('date' in val for val in row_values)
        if file_type == "bank":
            has_amount = any('credit' in val or 'debit' in val for val in row_values)
        else:
            has_amount = any('debit' in val for val in row_values)
        if has_date and has_amount:
            header_row = i
            break

    if header_row is None:
        print("WARNING: Could not find header row in data")
        return df, 0

    df_with_headers = df.iloc[header_row:].copy()
    df_with_headers.columns = df_with_headers.iloc[0]
    df_data = df_with_headers.iloc[1:].reset_index(drop=True)
    return df_data, header_row


def perform_reconciliation(bank_file, ledger_file, output_file):
    """Main reconciliation function with enhanced summary"""
    print("=" * 70)
    print("BANK RECONCILIATION SYSTEM - ENHANCED VERSION")
    print("=" * 70)

    # Load files
    bank_df_raw = pd.read_excel(bank_file, header=None) if bank_file.lower().endswith(('.xlsx', '.xls')) else pd.read_csv(bank_file, header=None)
    ledger_df_raw = pd.read_excel(ledger_file, header=None) if ledger_file.lower().endswith(('.xlsx', '.xls')) else pd.read_csv(ledger_file, header=None)

    # Extract valid data
    bank_df, _ = find_actual_data_rows(bank_df_raw, "bank")
    ledger_df, _ = find_actual_data_rows(ledger_df_raw, "ledger")

    print(f"\nBank records: {len(bank_df)}")
    print(f"Ledger records: {len(ledger_df)}")

    # Identify key columns
    bank_date_col, bank_credit_col = find_value_date_and_amount_columns(bank_df, "bank")
    ledger_date_col, ledger_debit_col = find_value_date_and_amount_columns(ledger_df, "ledger")

    if not all([bank_date_col, bank_credit_col, ledger_date_col, ledger_debit_col]):
        print("❌ ERROR: Could not find required columns.")
        return

    print(f"\n[SUCCESS] Bank Date Column: {bank_date_col}")
    print(f"[SUCCESS] Bank Credit Column: {bank_credit_col}")
    print(f"[SUCCESS] Ledger Date Column: {ledger_date_col}")
    print(f"[SUCCESS] Ledger Debit Column: {ledger_debit_col}")

    # Prepare data
    bank_work = bank_df.copy()
    ledger_work = ledger_df.copy()

    bank_work['clean_date'] = pd.to_datetime(bank_work[bank_date_col], errors='coerce')
    ledger_work['clean_date'] = pd.to_datetime(ledger_work[ledger_date_col], errors='coerce')

    bank_work['internal_amount'] = pd.to_numeric(bank_work[bank_credit_col].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce')
    ledger_work['internal_amount'] = pd.to_numeric(ledger_work[ledger_debit_col].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce')

    bank_work['match_date'] = bank_work['clean_date'].dt.strftime('%Y-%m-%d')
    ledger_work['match_date'] = ledger_work['clean_date'].dt.strftime('%Y-%m-%d')

    bank_work['match_amount'] = bank_work['internal_amount'].abs().round(2)
    ledger_work['match_amount'] = ledger_work['internal_amount'].abs().round(2)

    bank_work['original_bank_index'] = bank_work.index
    ledger_work['original_ledger_index'] = ledger_work.index

    # Filter valid rows
    bank_valid = bank_work.dropna(subset=['match_date', 'match_amount'])
    ledger_valid = ledger_work.dropna(subset=['match_date', 'match_amount'])
    bank_valid = bank_valid[bank_valid['match_amount'] != 0]
    ledger_valid = ledger_valid[ledger_valid['match_amount'] != 0]

    print("\nPerforming reconciliation (Date + Amount matching)...")

    # Merge on date + amount
    matches = pd.merge(
        bank_valid[['match_date', 'match_amount', 'original_bank_index']],
        ledger_valid[['match_date', 'match_amount', 'original_ledger_index']],
        on=['match_date', 'match_amount'],
        how='inner'
    )

    matched_bank_indices = set(matches['original_bank_index'])
    matched_ledger_indices = set(matches['original_ledger_index'])

    # Assign Status
    bank_df['Status'] = np.where(bank_df.index.isin(matched_bank_indices), 'Matched', 'Unmatched')
    ledger_df['Status'] = np.where(ledger_df.index.isin(matched_ledger_indices), 'Matched', 'Unmatched')

    # Calculate summary metrics
    total_bank = len(bank_df)
    matched_bank_count = len(matched_bank_indices)
    unmatched_bank_count = total_bank - matched_bank_count
    
    total_ledger = len(ledger_df)
    matched_ledger_count = len(matched_ledger_indices)
    unmatched_ledger_count = total_ledger - matched_ledger_count

    # Display summary
    print(f"\nBank matched: {matched_bank_count}/{total_bank} ({(matched_bank_count/total_bank*100) if total_bank > 0 else 0:.2f}%)")
    print(f"Ledger matched: {matched_ledger_count}/{total_ledger} ({(matched_ledger_count/total_ledger*100) if total_ledger > 0 else 0:.2f}%)")

    # Save Excel output with enhanced summary
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Enhanced Summary Sheet
        summary_data = [
            {'Metric': 'RECONCILIATION SUMMARY', 'Value': ''},
            {'Metric': 'Matching Strategy', 'Value': 'Date + Amount Matching'},
            {'Metric': '', 'Value': ''},
            {'Metric': 'BANK STATEMENT', 'Value': ''},
            {'Metric': 'Total Records', 'Value': total_bank},
            {'Metric': 'Matched', 'Value': matched_bank_count},
            {'Metric': 'Unmatched', 'Value': unmatched_bank_count},
            {'Metric': 'Match Rate', 'Value': f"{(matched_bank_count/total_bank*100) if total_bank > 0 else 0:.2f}%"},
            {'Metric': '', 'Value': ''},
            {'Metric': 'LEDGER', 'Value': ''},
            {'Metric': 'Total Records', 'Value': total_ledger},
            {'Metric': 'Matched', 'Value': matched_ledger_count},
            {'Metric': 'Unmatched', 'Value': unmatched_ledger_count},
            {'Metric': 'Match Rate', 'Value': f"{(matched_ledger_count/total_ledger*100) if total_ledger > 0 else 0:.2f}%"},
        ]
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Function to insert 3 blank columns before Status
        def insert_blank_cols_before_status(df):
            if 'Status' in df.columns:
                status_idx = df.columns.get_loc('Status')
                # Create 3 new blank columns with different temporary names to avoid conflicts
                df.insert(status_idx, 'temp_col_1', ['' for _ in range(len(df))])
                df.insert(status_idx + 1, 'temp_col_2', ['' for _ in range(len(df))])
                df.insert(status_idx + 2, 'temp_col_3', ['' for _ in range(len(df))])
                # Rename them to empty strings, ensuring they're different
                df.rename(columns={
                    'temp_col_1': '', 
                    'temp_col_2': ' ', 
                    'temp_col_3': '  '
                }, inplace=True)
            return df

        # Bank Statement Sheets
        bank_df_with_blanks = insert_blank_cols_before_status(bank_df.copy())
        bank_df_with_blanks.to_excel(writer, sheet_name='Bank - All', index=False)
        
        bank_matched = bank_df[bank_df['Status'] == 'Matched'].copy()
        bank_matched_with_blanks = insert_blank_cols_before_status(bank_matched)
        bank_matched_with_blanks.to_excel(writer, sheet_name='Bank - Matched', index=False)
        
        bank_unmatched = bank_df[bank_df['Status'] == 'Unmatched'].copy()
        bank_unmatched_with_blanks = insert_blank_cols_before_status(bank_unmatched)
        bank_unmatched_with_blanks.to_excel(writer, sheet_name='Bank - Unmatched', index=False)

        # Ledger Sheets
        ledger_df_with_blanks = insert_blank_cols_before_status(ledger_df.copy())
        ledger_df_with_blanks.to_excel(writer, sheet_name='Ledger - All', index=False)
        
        ledger_matched = ledger_df[ledger_df['Status'] == 'Matched'].copy()
        ledger_matched_with_blanks = insert_blank_cols_before_status(ledger_matched)
        ledger_matched_with_blanks.to_excel(writer, sheet_name='Ledger - Matched', index=False)
        
        ledger_unmatched = ledger_df[ledger_df['Status'] == 'Unmatched'].copy()
        ledger_unmatched_with_blanks = insert_blank_cols_before_status(ledger_unmatched)
        ledger_unmatched_with_blanks.to_excel(writer, sheet_name='Ledger - Unmatched', index=False)

    print(f"\n[SUCCESS] Results saved to: {output_file}")
    print("\nSheets created:")
    print("  1. Summary - Detailed reconciliation metrics")
    print("  2. Bank - All - Complete bank statement")
    print("  3. Bank - Matched - Matched bank transactions")
    print("  4. Bank - Unmatched - Unmatched bank transactions")
    print("  5. Ledger - All - Complete ledger")
    print("  6. Ledger - Matched - Matched ledger entries")
    print("  7. Ledger - Unmatched - Unmatched ledger entries")
    print("=" * 70)
    print("RECONCILIATION COMPLETE")
    print("=" * 70)


def main():
    """Main entry point"""
    BANK_FILE = os.getenv('BANK_STATEMENT_FILE_PATH', 'sample_bank_statement.xlsx')
    LEDGER_FILE = os.getenv('LEDGER_FILE_PATH', 'sample_ledger.xlsx')
    OUTPUT_FILE = os.getenv('OUTPUT_FILE_PATH', 'Reconciliation_Results.xlsx')

    print(f"DEBUG: BANK_FILE = {BANK_FILE}")
    print(f"DEBUG: LEDGER_FILE = {LEDGER_FILE}")
    print(f"DEBUG: OUTPUT_FILE = {OUTPUT_FILE}")

    perform_reconciliation(BANK_FILE, LEDGER_FILE, OUTPUT_FILE)


if __name__ == "__main__":
    main()

















# import pandas as pd
# import numpy as np
# from datetime import datetime
# import os
# from dotenv import load_dotenv
# from pathlib import Path

# # Explicitly load the .env file from the script directory first
# script_dir = Path(__file__).parent
# env_path = script_dir / '.env'
# print(f"Explicitly loading .env file from: {env_path.absolute()}")
# load_dotenv(env_path, override=True)

# def find_value_date_and_amount_columns(df, file_type):
#     """
#     Find Value Date and Credit/Debit columns in the dataframe.
#     """
#     date_col = None
#     amount_col = None
    
#     # Look for exact "Value Date" column
#     for col in df.columns:
#         if str(col).strip().lower() == 'value date':
#             date_col = col
#             break
    
#     # If not found, check for common variations
#     if date_col is None:
#         for col in df.columns:
#             col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
#             if col_clean in ['valuedate', 'value_date', 'date', 'transdate', 'transactiondate']:
#                 date_col = col
#                 break
    
#     # Find Credit (for bank) or Debit (for ledger) column
#     if file_type == "bank":
#         for col in df.columns:
#             if str(col).strip().lower() == 'credit':
#                 amount_col = col
#                 break
#         if amount_col is None:
#             for col in df.columns:
#                 col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
#                 if col_clean in ['credit', 'cr', 'credits', 'amount']:
#                     amount_col = col
#                     break
#     elif file_type == "ledger":
#         for col in df.columns:
#             if str(col).strip().lower() == 'debit':
#                 amount_col = col
#                 break
#         if amount_col is None:
#             for col in df.columns:
#                 col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
#                 if col_clean in ['debit', 'dr', 'debits', 'withdrawal', 'amount']:
#                     amount_col = col
#                     break
    
#     return date_col, amount_col

# def find_description_column(df):
#     """
#     Find a description/remarks/narration column for enhanced matching.
#     """
#     desc_keywords = ['remarks', 'description', 'narration', 'particulars', 'details', 'memo']
    
#     for col in df.columns:
#         col_lower = str(col).strip().lower()
#         for keyword in desc_keywords:
#             if keyword in col_lower:
#                 return col
#     return None

# def is_numeric_value(val):
#     """Helper function to check if a value is a valid non-zero number"""
#     if pd.isna(val):
#         return False
    
#     val_str = str(val).strip().replace(',', '').replace(' ', '')
    
#     # Check for empty or null strings
#     if not val_str or val_str.lower() in ['', 'nan', 'none', 'null', '#n/a']:
#         return False
    
#     # Try to convert to float
#     try:
#         num_val = float(val_str)
#         return not pd.isna(num_val) and num_val != 0
#     except (ValueError, TypeError):
#         return False

# def find_actual_data_rows(df, file_type):
#     """
#     Find the actual transaction data rows by identifying the header row and filtering out 
#     non-transaction rows like summaries, totals, etc.
#     """
#     # Find header rows by looking for "Value Date", "Credit", "Debit" keywords
#     header_row = None
    
#     for i in range(min(50, len(df))):
#         row_values = df.iloc[i].astype(str).str.lower()
        
#         has_date = any('date' in val for val in row_values)
        
#         if file_type == "bank":
#             has_amount = any('credit' in val or 'debit' in val for val in row_values)
#         else:  # ledger
#             has_amount = any('debit' in val for val in row_values)
        
#         if has_date and has_amount:
#             header_row = i
#             break

#     if header_row is None:
#         print("WARNING: Could not find header row in data")
#         return df, 0

#     # Set headers and get data after header row
#     df_with_headers = df.iloc[header_row:].copy()
#     df_with_headers.columns = df_with_headers.iloc[0]
#     df_data = df_with_headers.iloc[1:].reset_index(drop=True)
    
#     # Filter out completely empty rows
#     def is_row_empty(row):
#         """Check if a row is completely empty (all NaN or whitespace)"""
#         for val in row.values:
#             if pd.notna(val):
#                 val_str = str(val).strip()
#                 if val_str and val_str not in ['', 'nan', 'NaN', 'None', 'null', '#N/A', 'N/A']:
#                     return False
#         return True
    
#     empty_mask = df_data.apply(is_row_empty, axis=1)
#     rows_before = len(df_data)
#     df_data = df_data[~empty_mask].reset_index(drop=True)
#     rows_removed = rows_before - len(df_data)
    
#     if rows_removed > 0:
#         print(f"   Removed {rows_removed} completely empty rows")
    
#     # Filter out rows that are likely summaries or totals
#     summary_keywords = [
#         'total', 'grand total', 'sub total', 'subtotal', 'summary', 
#         'closing balance', 'opening balance', 'balance c/f', 'balance b/f', 
#         'overall total', 'balance forward', 'balance carried forward'
#     ]
    
#     non_summary_mask = pd.Series([True] * len(df_data), index=df_data.index)
    
#     for idx, row in df_data.iterrows():
#         row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
#         row_str_lower = row_str.lower()
        
#         is_summary = False
#         for keyword in summary_keywords:
#             if keyword in row_str_lower:
#                 clean_row_str = ' '.join(row_str_lower.split())
#                 if len(clean_row_str) < 50 or f' {keyword} ' in f' {clean_row_str} ':
#                     is_summary = True
#                     break
        
#         if is_summary:
#             non_summary_mask[idx] = False
    
#     # Validate amounts
#     valid_amount_mask = pd.Series([False] * len(df_data), index=df_data.index)
    
#     if file_type == "bank":
#         credit_col = None
#         for col in df_data.columns:
#             if 'credit' in str(col).lower():
#                 credit_col = col
#                 break
        
#         if credit_col and credit_col in df_data.columns:
#             for idx in df_data.index:
#                 if is_numeric_value(df_data.loc[idx, credit_col]):
#                     valid_amount_mask[idx] = True
        
#     else:  # ledger
#         debit_col = None
#         for col in df_data.columns:
#             if 'debit' in str(col).lower():
#                 debit_col = col
#                 break
        
#         if debit_col and debit_col in df_data.columns:
#             for idx in df_data.index:
#                 if is_numeric_value(df_data.loc[idx, debit_col]):
#                     valid_amount_mask[idx] = True
    
#     # Final mask: non-summary rows with valid amounts
#     final_mask = non_summary_mask & valid_amount_mask
    
#     print(f"\n{file_type.upper()} Filtering Results:")
#     print(f"   Rows after header extraction: {len(df_data)}")
#     print(f"   Rows after summary filter: {non_summary_mask.sum()}")
#     print(f"   Rows with valid amounts: {valid_amount_mask.sum()}")
#     print(f"   Final valid rows: {final_mask.sum()}")
    
#     return df_data[final_mask], header_row


# def perform_reconciliation(bank_file, ledger_file, output_file):
#     """
#     Multi-pass reconciliation with duplicate handling:
#     1. First pass: Exact match (date + amount)
#     2. Second pass: All remaining treated as legitimate duplicates
#     """
#     print("="*70)
#     print("BANK RECONCILIATION SYSTEM - ENHANCED VERSION")
#     print("="*70)
#     print("Multi-Pass Matching: Handles Legitimate Duplicates")
#     print("="*70)
    
#     # ========== LOAD FILES ==========
#     print(f"\nLoading Bank Statement: {bank_file}")
#     if bank_file.lower().endswith('.xlsx') or bank_file.lower().endswith('.xls'):
#         bank_df_raw = pd.read_excel(bank_file, header=None)
#     else:
#         bank_df_raw = pd.read_csv(bank_file, header=None)
    
#     print(f"Loading Ledger: {ledger_file}")
#     if ledger_file.lower().endswith('.xlsx') or ledger_file.lower().endswith('.xls'):
#         ledger_df_raw = pd.read_excel(ledger_file, header=None)
#     else:
#         ledger_df_raw = pd.read_csv(ledger_file, header=None)
    
#     # ========== EXTRACT TRANSACTION DATA ==========
#     print("\nExtracting transaction data...")
#     bank_df, _ = find_actual_data_rows(bank_df_raw, "bank")
#     ledger_df, _ = find_actual_data_rows(ledger_df_raw, "ledger")
    
#     print(f"\n   Bank records: {len(bank_df)}")
#     print(f"   Ledger records: {len(ledger_df)}")
    
#     # ========== FIND COLUMNS ==========
#     bank_date_col, bank_credit_col = find_value_date_and_amount_columns(bank_df, "bank")
#     ledger_date_col, ledger_debit_col = find_value_date_and_amount_columns(ledger_df, "ledger")
    
#     # Try to find description columns
#     bank_desc_col = find_description_column(bank_df)
#     ledger_desc_col = find_description_column(ledger_df)
    
#     if not all([bank_date_col, bank_credit_col, ledger_date_col, ledger_debit_col]):
#         print("❌ ERROR: Could not find required columns")
#         print(f"   Bank: Date={bank_date_col}, Credit={bank_credit_col}")
#         print(f"   Ledger: Date={ledger_date_col}, Debit={ledger_debit_col}")
#         return
    
#     print(f"\n✓ Bank Date Column: {bank_date_col}")
#     print(f"✓ Bank Credit Column: {bank_credit_col}")
#     if bank_desc_col:
#         print(f"✓ Bank Description Column: {bank_desc_col}")
#     print(f"✓ Ledger Date Column: {ledger_date_col}")
#     print(f"✓ Ledger Debit Column: {ledger_debit_col}")
#     if ledger_desc_col:
#         print(f"✓ Ledger Description Column: {ledger_desc_col}")
    
#     # ========== PREPARE DATA ==========
#     print("\n" + "="*70)
#     print("PREPARING DATA FOR MATCHING...")
#     print("="*70)
    
#     # Create copies to work with
#     bank_work = bank_df.copy()
#     ledger_work = ledger_df.copy()
    
#     # Convert date columns to datetime
#     bank_work['clean_date'] = pd.to_datetime(bank_work[bank_date_col], errors='coerce')
#     ledger_work['clean_date'] = pd.to_datetime(ledger_work[ledger_date_col], errors='coerce')
    
#     # Convert amount columns to numeric
#     bank_work['internal_amount'] = pd.to_numeric(
#         bank_work[bank_credit_col].astype(str).str.replace(',', '').str.replace(' ', ''), 
#         errors='coerce'
#     )
#     ledger_work['internal_amount'] = pd.to_numeric(
#         ledger_work[ledger_debit_col].astype(str).str.replace(',', '').str.replace(' ', ''), 
#         errors='coerce'
#     )
    
#     # Create match keys
#     bank_work['match_date'] = bank_work['clean_date'].dt.strftime('%Y-%m-%d')
#     ledger_work['match_date'] = ledger_work['clean_date'].dt.strftime('%Y-%m-%d')
    
#     bank_work['match_amount'] = bank_work['internal_amount'].abs().round(2)
#     ledger_work['match_amount'] = ledger_work['internal_amount'].abs().round(2)
    
#     # Add description for enhanced matching if available
#     if bank_desc_col:
#         bank_work['match_desc'] = bank_work[bank_desc_col].astype(str).str.strip().str.lower()
#     else:
#         bank_work['match_desc'] = ''
    
#     if ledger_desc_col:
#         ledger_work['match_desc'] = ledger_work[ledger_desc_col].astype(str).str.strip().str.lower()
#     else:
#         ledger_work['match_desc'] = ''
    
#     # Store original indices
#     bank_work['original_bank_index'] = bank_work.index
#     ledger_work['original_ledger_index'] = ledger_work.index
    
#     # Filter out rows with invalid dates or amounts
#     bank_valid = bank_work.dropna(subset=['match_date', 'match_amount']).copy()
#     ledger_valid = ledger_work.dropna(subset=['match_date', 'match_amount']).copy()
    
#     # Remove rows where match_amount is 0
#     bank_valid = bank_valid[bank_valid['match_amount'] != 0]
#     ledger_valid = ledger_valid[ledger_valid['match_amount'] != 0]
    
#     print(f"\n[DEBUG] Valid bank records for matching: {len(bank_valid)}")
#     print(f"[DEBUG] Valid ledger records for matching: {len(ledger_valid)}")
    
#     # ========== MULTI-PASS MATCHING ==========
#     print("\n" + "="*70)
#     print("PERFORMING MULTI-PASS RECONCILIATION...")
#     print("="*70)
    
#     matched_bank_indices = set()
#     matched_ledger_indices = set()
    
#     # PASS 1: Exact match with description (if available)
#     if bank_desc_col and ledger_desc_col:
#         print("\n[PASS 1] Matching with Date + Amount + Description...")
        
#         # Create temporary match keys with description
#         bank_temp = bank_valid[bank_valid['match_desc'] != ''][
#             ['match_date', 'match_amount', 'match_desc', 'original_bank_index']
#         ].copy()
        
#         ledger_temp = ledger_valid[ledger_valid['match_desc'] != ''][
#             ['match_date', 'match_amount', 'match_desc', 'original_ledger_index']
#         ].copy()
        
#         if len(bank_temp) > 0 and len(ledger_temp) > 0:
#             # Match on date + amount + description
#             matches = pd.merge(
#                 bank_temp,
#                 ledger_temp,
#                 on=['match_date', 'match_amount', 'match_desc'],
#                 how='inner'
#             )
            
#             for _, match_row in matches.iterrows():
#                 b_idx = match_row['original_bank_index']
#                 l_idx = match_row['original_ledger_index']
                
#                 if b_idx not in matched_bank_indices and l_idx not in matched_ledger_indices:
#                     matched_bank_indices.add(b_idx)
#                     matched_ledger_indices.add(l_idx)
            
#             print(f"   ✓ Matched {len(matched_bank_indices)} transactions with description")
    
#     # PASS 2: Match remaining on Date + Amount only (handle ALL as legitimate)
#     print(f"\n[PASS 2] Matching remaining on Date + Amount...")
#     print("   Note: All same date/amount pairs will be matched (treating duplicates as legitimate)")
    
#     # Get unmatched records
#     bank_remaining = bank_valid[~bank_valid['original_bank_index'].isin(matched_bank_indices)].copy()
#     ledger_remaining = ledger_valid[~ledger_valid['original_ledger_index'].isin(matched_ledger_indices)].copy()
    
#     print(f"   Bank remaining: {len(bank_remaining)}")
#     print(f"   Ledger remaining: {len(ledger_remaining)}")
    
#     # Group by date and amount
#     if len(bank_remaining) > 0 and len(ledger_remaining) > 0:
#         bank_temp = bank_remaining[['match_date', 'match_amount', 'original_bank_index']].copy()
#         ledger_temp = ledger_remaining[['match_date', 'match_amount', 'original_ledger_index']].copy()
        
#         # Get all possible matches
#         all_matches = pd.merge(
#             bank_temp,
#             ledger_temp,
#             on=['match_date', 'match_amount'],
#             how='inner'
#         )
        
#         print(f"   Found {len(all_matches)} possible match combinations")
        
#         # Match ALL pairs (treating duplicates as legitimate transactions)
#         for _, match_row in all_matches.iterrows():
#             b_idx = match_row['original_bank_index']
#             l_idx = match_row['original_ledger_index']
            
#             if b_idx not in matched_bank_indices and l_idx not in matched_ledger_indices:
#                 matched_bank_indices.add(b_idx)
#                 matched_ledger_indices.add(l_idx)
    
#     print(f"   ✓ Total matched after Pass 2: {len(matched_bank_indices)}")
    
#     # ========== UPDATE STATUS ==========
#     bank_df['Status'] = 'Unmatched'
#     bank_df.loc[list(matched_bank_indices), 'Status'] = 'Matched'
    
#     ledger_df['Status'] = 'Unmatched'
#     ledger_df.loc[list(matched_ledger_indices), 'Status'] = 'Matched'
    
#     # Calculate statistics
#     total_bank = len(bank_df)
#     matched_bank_count = len(matched_bank_indices)
#     unmatched_bank_count = total_bank - matched_bank_count
    
#     total_ledger = len(ledger_df)
#     matched_ledger_count = len(matched_ledger_indices)
#     unmatched_ledger_count = total_ledger - matched_ledger_count
    
#     # ========== ANALYZE DUPLICATES ==========
#     print("\n" + "="*70)
#     print("DUPLICATE ANALYSIS")
#     print("="*70)
    
#     # Check for remaining duplicate scenarios
#     unmatched_bank = bank_df[bank_df['Status'] == 'Unmatched'].copy()
#     unmatched_ledger = ledger_df[ledger_df['Status'] == 'Unmatched'].copy()
    
#     if len(unmatched_bank) > 0:
#         unmatched_bank['temp_date'] = pd.to_datetime(unmatched_bank[bank_date_col], errors='coerce').dt.strftime('%Y-%m-%d')
#         unmatched_bank['temp_amt'] = pd.to_numeric(
#             unmatched_bank[bank_credit_col].astype(str).str.replace(',', ''), 
#             errors='coerce'
#         ).abs().round(2)
        
#         bank_dup_counts = unmatched_bank.groupby(['temp_date', 'temp_amt']).size()
#         bank_dups = bank_dup_counts[bank_dup_counts > 1]
        
#         if len(bank_dups) > 0:
#             print(f"\n⚠️  WARNING: {len(bank_dups)} unmatched bank date-amount groups have internal duplicates")
#             print("   (These might be data entry errors or require manual review)")
#             for (date, amt), count in list(bank_dups.items())[:5]:
#                 print(f"   - Date: {date}, Amount: {amt:,.2f}, Count: {count}")
    
#     # ========== DISPLAY RESULTS ==========
#     print("\n" + "="*70)
#     print("RECONCILIATION RESULTS")
#     print("="*70)
    
#     print(f"\nBANK STATEMENT:")
#     print(f"   Total records: {total_bank}")
#     print(f"   ✓ Matched: {matched_bank_count} ({matched_bank_count/total_bank*100:.1f}%)")
#     print(f"   ✗ Unmatched: {unmatched_bank_count} ({unmatched_bank_count/total_bank*100:.1f}%)")
    
#     print(f"\nLEDGER:")
#     print(f"   Total records: {total_ledger}")
#     print(f"   ✓ Matched: {matched_ledger_count} ({matched_ledger_count/total_ledger*100:.1f}%)")
#     print(f"   ✗ Unmatched: {unmatched_ledger_count} ({unmatched_ledger_count/total_ledger*100:.1f}%)")
    
#     # Show sample of unmatched transactions
#     print("\n" + "="*70)
#     print("SAMPLE UNMATCHED TRANSACTIONS")
#     print("="*70)
    
#     if len(unmatched_bank) > 0:
#         print(f"\nBank Unmatched (showing first 5 of {len(unmatched_bank)}):")
#         for idx in unmatched_bank.head(5).index:
#             date_val = bank_df.loc[idx, bank_date_col]
#             amount_val = bank_df.loc[idx, bank_credit_col]
#             print(f"   Date: {date_val}, Amount: {amount_val}")
    
#     if len(unmatched_ledger) > 0:
#         print(f"\nLedger Unmatched (showing first 5 of {len(unmatched_ledger)}):")
#         for idx in unmatched_ledger.head(5).index:
#             date_val = ledger_df.loc[idx, ledger_date_col]
#             amount_val = ledger_df.loc[idx, ledger_debit_col]
#             print(f"   Date: {date_val}, Amount: {amount_val}")
    
#     # ========== SAVE RESULTS ==========
#     print(f"\n[SAVING] Saving results to: {output_file}")
    
#     with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
#         # Summary sheet
#         summary_data = [
#             {'Metric': 'RECONCILIATION SUMMARY', 'Value': ''},
#             {'Metric': 'Matching Strategy', 'Value': 'Multi-Pass with Duplicate Handling'},
#             {'Metric': '', 'Value': ''},
#             {'Metric': 'BANK STATEMENT', 'Value': ''},
#             {'Metric': 'Total Records', 'Value': total_bank},
#             {'Metric': 'Matched', 'Value': matched_bank_count},
#             {'Metric': 'Unmatched', 'Value': unmatched_bank_count},
#             {'Metric': 'Match Rate', 'Value': f"{(matched_bank_count/total_bank*100) if total_bank > 0 else 0:.2f}%"},
#             {'Metric': '', 'Value': ''},
#             {'Metric': 'LEDGER', 'Value': ''},
#             {'Metric': 'Total Records', 'Value': total_ledger},
#             {'Metric': 'Matched', 'Value': matched_ledger_count},
#             {'Metric': 'Unmatched', 'Value': unmatched_ledger_count},
#             {'Metric': 'Match Rate', 'Value': f"{(matched_ledger_count/total_ledger*100) if total_ledger > 0 else 0:.2f}%"},
#         ]
        
#         summary_df = pd.DataFrame.from_records(summary_data)
#         summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
#         # Prepare columns for export
#         def prepare_columns(df):
#             cols = [c for c in df.columns if c not in ['clean_date', 'internal_amount', 
#                                                          'match_date', 'match_amount', 'match_desc',
#                                                          'original_bank_index', 'original_ledger_index',
#                                                          'temp_date', 'temp_amt']]
#             if 'Status' in cols:
#                 cols.remove('Status')
#                 cols.append('Status')
#             return cols
        
#         # Bank sheets
#         bank_cols = prepare_columns(bank_df)
#         bank_df[bank_cols].to_excel(writer, sheet_name='Bank Statement (All)', index=False)
#         bank_df[bank_df['Status'] == 'Matched'][bank_cols].to_excel(writer, sheet_name='Bank - Matched', index=False)
#         bank_df[bank_df['Status'] == 'Unmatched'][bank_cols].to_excel(writer, sheet_name='Bank - Unmatched', index=False)
        
#         # Ledger sheets
#         ledger_cols = prepare_columns(ledger_df)
#         ledger_df[ledger_cols].to_excel(writer, sheet_name='Ledger (All)', index=False)
#         ledger_df[ledger_df['Status'] == 'Matched'][ledger_cols].to_excel(writer, sheet_name='Ledger - Matched', index=False)
#         ledger_df[ledger_df['Status'] == 'Unmatched'][ledger_cols].to_excel(writer, sheet_name='Ledger - Unmatched', index=False)
    
#     print("\n[SUCCESS] Results saved successfully!")
#     print("\n" + "="*70)
#     print("RECONCILIATION COMPLETE!")
#     print("="*70)


# def main():
#     """
#     Main function that reads configuration from .env file.
#     """
#     BANK_FILE = os.getenv('BANK_STATEMENT_FILE_PATH', 'sample_bank_statement.xlsx')
#     LEDGER_FILE = os.getenv('LEDGER_FILE_PATH', 'sample_ledger.xlsx')
#     OUTPUT_FILE = os.getenv('OUTPUT_FILE_PATH', 'Reconciliation_Results.xlsx')
    
#     print(f"DEBUG: Loaded BANK_FILE = {BANK_FILE}")
#     print(f"DEBUG: Loaded LEDGER_FILE = {LEDGER_FILE}")
#     print(f"DEBUG: Loaded OUTPUT_FILE = {OUTPUT_FILE}")
    
#     perform_reconciliation(BANK_FILE, LEDGER_FILE, OUTPUT_FILE)

# if __name__ == "__main__":
#     main()