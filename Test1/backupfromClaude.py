import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

# Explicitly load the .env file from the script directory first
script_dir = Path(__file__).parent
env_path = script_dir / '.env'
print(f"Explicitly loading .env file from: {env_path.absolute()}")
load_dotenv(env_path, override=True)

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
            col_clean = str(col).strip().lower().replace(' ', '').replace('_', '')
            if col_clean in ['valuedate', 'value_date', 'date', 'transdate', 'transactiondate']:
                date_col = col
                break
    
    # Find Credit (for bank) or Debit (for ledger) column
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

def is_numeric_value(val):
    """Helper function to check if a value is a valid non-zero number"""
    if pd.isna(val):
        return False
    
    val_str = str(val).strip().replace(',', '').replace(' ', '')
    
    # Check for empty or null strings
    if not val_str or val_str.lower() in ['', 'nan', 'none', 'null', '#n/a']:
        return False
    
    # Try to convert to float
    try:
        num_val = float(val_str)
        return not pd.isna(num_val) and num_val != 0
    except (ValueError, TypeError):
        return False

def find_actual_data_rows(df, file_type):
    """
    Find the actual transaction data rows by identifying the header row and filtering out 
    non-transaction rows like summaries, totals, etc.
    """
    # Find header rows by looking for "Value Date", "Credit", "Debit" keywords
    header_row = None
    
    for i in range(min(50, len(df))):
        row_values = df.iloc[i].astype(str).str.lower()
        
        has_date = any('date' in val for val in row_values)
        
        if file_type == "bank":
            has_amount = any('credit' in val or 'debit' in val for val in row_values)
        else:  # ledger
            has_amount = any('debit' in val for val in row_values)
        
        if has_date and has_amount:
            header_row = i
            break

    if header_row is None:
        print("WARNING: Could not find header row in data")
        return df, 0

    # Set headers and get data after header row
    df_with_headers = df.iloc[header_row:].copy()
    df_with_headers.columns = df_with_headers.iloc[0]
    df_data = df_with_headers.iloc[1:].reset_index(drop=True)
    
    # Filter out completely empty rows
    df_data = df_data.dropna(how='all')
    
    # Filter out rows that are likely summaries or totals
    summary_keywords = [
        'total', 'grand total', 'sub total', 'subtotal', 'summary', 
        'closing balance', 'opening balance', 'balance c/f', 'balance b/f', 
        'overall total', 'balance forward', 'balance carried forward'
    ]
    
    non_summary_mask = pd.Series([True] * len(df_data), index=df_data.index)
    
    for idx, row in df_data.iterrows():
        row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
        row_str_lower = row_str.lower()
        
        is_summary = False
        for keyword in summary_keywords:
            if keyword in row_str_lower:
                # For short rows or rows where keyword appears as a standalone word
                clean_row_str = ' '.join(row_str_lower.split())
                if len(clean_row_str) < 50 or f' {keyword} ' in f' {clean_row_str} ':
                    is_summary = True
                    break
        
        if is_summary:
            non_summary_mask[idx] = False
    
    # OPTIMIZED AMOUNT VALIDATION - Check if row has ANY valid numeric value in amount columns
    valid_amount_mask = pd.Series([False] * len(df_data), index=df_data.index)
    
    if file_type == "bank":
        # For bank statements, check BOTH Credit AND Debit columns
        credit_col = None
        debit_col = None
        
        for col in df_data.columns:
            col_lower = str(col).lower()
            if 'credit' in col_lower and credit_col is None:
                credit_col = col
            if 'debit' in col_lower and debit_col is None:
                debit_col = col
        
        print(f"   Bank Credit Column: {credit_col}")
        print(f"   Bank Debit Column: {debit_col}")
        
        # Check each row
        for idx in df_data.index:
            has_valid_amount = False
            
            # Check Credit column
            if credit_col and credit_col in df_data.columns:
                if is_numeric_value(df_data.loc[idx, credit_col]):
                    has_valid_amount = True
            
            # Check Debit column (if Credit was empty)
            if not has_valid_amount and debit_col and debit_col in df_data.columns:
                if is_numeric_value(df_data.loc[idx, debit_col]):
                    has_valid_amount = True
            
            valid_amount_mask[idx] = has_valid_amount
        
    else:  # file_type == "ledger"
        # For ledgers, check Debit column
        debit_col = None
        for col in df_data.columns:
            if 'debit' in str(col).lower():
                debit_col = col
                break
        
        print(f"   Ledger Debit Column: {debit_col}")
        
        if debit_col and debit_col in df_data.columns:
            for idx in df_data.index:
                if is_numeric_value(df_data.loc[idx, debit_col]):
                    valid_amount_mask[idx] = True
    
    # Final mask: non-summary rows with valid amounts
    final_mask = non_summary_mask & valid_amount_mask
    
    print(f"\nDEBUG - Filtering Results:")
    print(f"   Rows after header extraction: {len(df_data)}")
    print(f"   Rows after summary filter: {non_summary_mask.sum()}")
    print(f"   Rows with valid amounts: {valid_amount_mask.sum()}")
    print(f"   Final valid rows: {final_mask.sum()}")
    
    return df_data[final_mask], header_row


def perform_matching(bank_df, ledger_df, bank_date_col, bank_credit_col, ledger_date_col, ledger_debit_col, stage_number):
    """
    Perform matching between bank and ledger for a specific stage.
    Returns matched indices for both bank and ledger.
    """
    # Create copies to avoid modifying originals
    bank_work = bank_df.copy()
    ledger_work = ledger_df.copy()
    
    # Convert date columns to datetime
    bank_work['clean_date'] = pd.to_datetime(bank_work[bank_date_col], errors='coerce')
    ledger_work['clean_date'] = pd.to_datetime(ledger_work[ledger_date_col], errors='coerce')
    
    # Convert amount columns to numeric (handle commas and spaces)
    bank_work['internal_amount'] = pd.to_numeric(
        bank_work[bank_credit_col].astype(str).str.replace(',', '').str.replace(' ', ''), 
        errors='coerce'
    )
    ledger_work['internal_amount'] = pd.to_numeric(
        ledger_work[ledger_debit_col].astype(str).str.replace(',', '').str.replace(' ', ''), 
        errors='coerce'
    )
    
    # Create match keys
    bank_work['match_date'] = bank_work['clean_date'].dt.strftime('%Y-%m-%d')
    ledger_work['match_date'] = ledger_work['clean_date'].dt.strftime('%Y-%m-%d')
    
    bank_work['match_amount'] = bank_work['internal_amount'].abs().round(2)
    ledger_work['match_amount'] = ledger_work['internal_amount'].abs().round(2)
    
    # Store original indices
    bank_work['original_bank_index'] = bank_work.index
    ledger_work['original_ledger_index'] = ledger_work.index
    
    # Filter out rows with NaT dates or NaN amounts
    bank_valid = bank_work.dropna(subset=['match_date', 'match_amount'])
    ledger_valid = ledger_work.dropna(subset=['match_date', 'match_amount'])
    
    # Create temporary DataFrames for matching
    bank_temp = bank_valid[['match_date', 'match_amount', 'original_bank_index']].copy()
    ledger_temp = ledger_valid[['match_date', 'match_amount', 'original_ledger_index']].copy()
    
    # Perform the merge to find ALL possible matches
    all_matches = pd.merge(
        bank_temp,
        ledger_temp,
        on=['match_date', 'match_amount'],
        how='inner',
        suffixes=('_bank', '_ledger')
    )
    
    # Implement one-to-one matching
    matched_bank_indices = set()
    matched_ledger_indices = set()
    
    # Handle column names
    bank_idx_col = 'original_bank_index' if 'original_bank_index' in all_matches.columns else 'original_bank_index_bank'
    ledger_idx_col = 'original_ledger_index' if 'original_ledger_index' in all_matches.columns else 'original_ledger_index_ledger'
    
    for _, match_row in all_matches.iterrows():
        bank_idx = match_row[bank_idx_col]
        ledger_idx = match_row[ledger_idx_col]
        
        if bank_idx not in matched_bank_indices and ledger_idx not in matched_ledger_indices:
            matched_bank_indices.add(bank_idx)
            matched_ledger_indices.add(ledger_idx)
    
    return list(matched_bank_indices), list(matched_ledger_indices)


def two_stage_reconciliation(bank_file, ledger1_file, ledger2_file, output_file):
    """
    Two-stage reconciliation:
    Stage 1: Match bank with Ledger 1
    Stage 2: Match unmatched bank records with Ledger 2 (General Ledger)
    """
    print("="*70)
    print("TWO-STAGE BANK RECONCILIATION SYSTEM")
    print("="*70)
    print("STAGE 1: Match Bank Statement with Primary Ledger")
    print("STAGE 2: Match Unmatched Bank Records with Secondary/General Ledger")
    print("="*70)
    
    # ========== LOAD ALL FILES ==========
    print(f"\nLoading Bank Statement: {bank_file}")
    if bank_file.lower().endswith('.xlsx') or bank_file.lower().endswith('.xls'):
        bank_df_raw = pd.read_excel(bank_file, header=None)
    else:
        bank_df_raw = pd.read_csv(bank_file, header=None)
    
    print(f"Loading Primary Ledger: {ledger1_file}")
    if ledger1_file.lower().endswith('.xlsx') or ledger1_file.lower().endswith('.xls'):
        ledger1_df_raw = pd.read_excel(ledger1_file, header=None)
    else:
        ledger1_df_raw = pd.read_csv(ledger1_file, header=None)
    
    print(f"Loading Secondary/General Ledger: {ledger2_file}")
    if ledger2_file.lower().endswith('.xlsx') or ledger2_file.lower().endswith('.xls'):
        ledger2_df_raw = pd.read_excel(ledger2_file, header=None)
    else:
        ledger2_df_raw = pd.read_csv(ledger2_file, header=None)
    
    # ========== EXTRACT TRANSACTION DATA ==========
    print("\nExtracting transaction data...")
    bank_df, _ = find_actual_data_rows(bank_df_raw, "bank")
    ledger1_df, _ = find_actual_data_rows(ledger1_df_raw, "ledger")
    ledger2_df, _ = find_actual_data_rows(ledger2_df_raw, "ledger")
    
    print(f"   Bank records: {len(bank_df)}")
    print(f"   Ledger 1 records: {len(ledger1_df)}")
    print(f"   Ledger 2 records: {len(ledger2_df)}")
    
    # ========== FIND COLUMNS ==========
    bank_date_col, bank_credit_col = find_value_date_and_amount_columns(bank_df, "bank")
    ledger1_date_col, ledger1_debit_col = find_value_date_and_amount_columns(ledger1_df, "ledger")
    ledger2_date_col, ledger2_debit_col = find_value_date_and_amount_columns(ledger2_df, "ledger")
    
    if not all([bank_date_col, bank_credit_col, ledger1_date_col, ledger1_debit_col, ledger2_date_col, ledger2_debit_col]):
        print("❌ ERROR: Could not find required columns in one or more files")
        print(f"   Bank: Date={bank_date_col}, Credit={bank_credit_col}")
        print(f"   Ledger1: Date={ledger1_date_col}, Debit={ledger1_debit_col}")
        print(f"   Ledger2: Date={ledger2_date_col}, Debit={ledger2_debit_col}")
        return
    
    # ========== STAGE 1: BANK vs LEDGER 1 ==========
    print("\n" + "="*70)
    print("STAGE 1: Matching Bank Statement with Primary Ledger")
    print("="*70)
    
    matched_bank_stage1, matched_ledger1 = perform_matching(
        bank_df, ledger1_df,
        bank_date_col, bank_credit_col,
        ledger1_date_col, ledger1_debit_col,
        stage_number=1
    )
    
    # Initialize Status_1 for all dataframes
    bank_df['Status_1'] = 'Unmatched_Stage1'
    bank_df.loc[matched_bank_stage1, 'Status_1'] = 'Matched_Stage1'
    
    ledger1_df['Status_1'] = 'Unmatched_Stage1'
    ledger1_df.loc[matched_ledger1, 'Status_1'] = 'Matched_Stage1'
    
    # Stage 1 results
    total_bank = len(bank_df)
    matched_stage1_count = len(matched_bank_stage1)
    unmatched_stage1_count = total_bank - matched_stage1_count
    
    print(f"\n[SUCCESS] Stage 1 Results:")
    print(f"   Total Bank records: {total_bank}")
    print(f"   Matched with Ledger 1: {matched_stage1_count}")
    print(f"   Unmatched (going to Stage 2): {unmatched_stage1_count}")
    
    # ========== STAGE 2: UNMATCHED BANK vs LEDGER 2 ==========
    print("\n" + "="*70)
    print("STAGE 2: Matching Unmatched Bank Records with Secondary Ledger")
    print("="*70)
    
    bank_unmatched_stage1 = bank_df[bank_df['Status_1'] == 'Unmatched_Stage1'].copy()
    bank_df['Status_2'] = ''
    
    if len(bank_unmatched_stage1) == 0:
        print("[SUCCESS] All bank records matched in Stage 1. No Stage 2 needed.")
        matched_stage2_count = 0
        unmatched_stage2_count = 0
        matched_ledger2 = []
        ledger2_df['Status_2'] = ''
    else:
        matched_bank_stage2_indices, matched_ledger2 = perform_matching(
            bank_unmatched_stage1, ledger2_df,
            bank_date_col, bank_credit_col,
            ledger2_date_col, ledger2_debit_col,
            stage_number=2
        )
        
        bank_df.loc[bank_df['Status_1'] == 'Unmatched_Stage1', 'Status_2'] = 'Unmatched_Stage2'
        if matched_bank_stage2_indices:
            bank_df.loc[matched_bank_stage2_indices, 'Status_2'] = 'Matched_Stage2'
        
        ledger2_df['Status_2'] = 'Unmatched_Stage2'
        if matched_ledger2:
            ledger2_df.loc[matched_ledger2, 'Status_2'] = 'Matched_Stage2'
        
        matched_stage2_count = len(matched_bank_stage2_indices)
        unmatched_stage2_count = unmatched_stage1_count - matched_stage2_count
        
        print(f"\n[SUCCESS] Stage 2 Results:")
        print(f"   Unmatched from Stage 1: {unmatched_stage1_count}")
        print(f"   Matched with Ledger 2: {matched_stage2_count}")
        print(f"   Still Unmatched: {unmatched_stage2_count}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "="*70)
    print("FINAL RECONCILIATION SUMMARY")
    print("="*70)
    print(f"\nBANK STATEMENT:")
    print(f"   Total records: {total_bank}")
    print(f"   + Matched in Stage 1 (Ledger 1): {matched_stage1_count} ({matched_stage1_count/total_bank*100:.1f}%)")
    print(f"   + Matched in Stage 2 (Ledger 2): {matched_stage2_count} ({matched_stage2_count/total_bank*100:.1f}%)")
    print(f"   - Still Unmatched: {unmatched_stage2_count} ({unmatched_stage2_count/total_bank*100:.1f}%)")
    print(f"\n   Overall Match Rate: {(matched_stage1_count + matched_stage2_count)/total_bank*100:.1f}%")
    
    print(f"\nLEDGER 1 (Primary):")
    print(f"   Total records: {len(ledger1_df)}")
    print(f"   + Matched with Bank: {len(matched_ledger1)}")
    print(f"   - Unmatched: {len(ledger1_df) - len(matched_ledger1)}")
    
    print(f"\nLEDGER 2 (Secondary/General):")
    print(f"   Total records: {len(ledger2_df)}")
    print(f"   + Matched with Bank: {len(matched_ledger2)}")
    print(f"   - Unmatched: {len(ledger2_df) - len(matched_ledger2)}")
    
    # ========== SAVE RESULTS ==========
    print(f"\n[SAVING] Saving results to: {output_file}")
    
    # Add gap columns for visual separation
    for df in [bank_df, ledger1_df, ledger2_df]:
        df[' '] = ''
        df['  '] = ''
        df['   '] = ''
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet
        ledger1_unmatched = len(ledger1_df) - len(matched_ledger1)
        ledger2_unmatched = len(ledger2_df) - len(matched_ledger2)
        
        summary_data = [
            {'Metric': 'BANK STATEMENT SUMMARY', 'Value': ''},
            {'Metric': 'Total Bank Statement Records', 'Value': total_bank},
            {'Metric': '', 'Value': ''},
            {'Metric': 'STAGE 1: Matching with Primary Ledger', 'Value': ''},
            {'Metric': 'Matched with Ledger 1', 'Value': matched_stage1_count},
            {'Metric': 'Unmatched with Ledger 1', 'Value': unmatched_stage1_count},
            {'Metric': 'Stage 1 Match Rate', 'Value': f"{(matched_stage1_count/total_bank*100) if total_bank > 0 else 0:.2f}%"},
            {'Metric': '', 'Value': ''},
            {'Metric': 'STAGE 2: Matching Unmatched with Secondary Ledger', 'Value': ''},
            {'Metric': 'Matched with Ledger 2', 'Value': matched_stage2_count},
            {'Metric': 'Still Unmatched after Stage 2', 'Value': unmatched_stage2_count},
            {'Metric': 'Stage 2 Match Rate', 'Value': f"{(matched_stage2_count/unmatched_stage1_count*100) if unmatched_stage1_count > 0 else 0:.2f}%"},
            {'Metric': '', 'Value': ''},
            {'Metric': 'OVERALL BANK RECONCILIATION', 'Value': ''},
            {'Metric': 'Total Matched (Stage 1 + Stage 2)', 'Value': matched_stage1_count + matched_stage2_count},
            {'Metric': 'Total Unmatched', 'Value': unmatched_stage2_count},
            {'Metric': 'Overall Match Rate', 'Value': f"{((matched_stage1_count + matched_stage2_count)/total_bank*100) if total_bank > 0 else 0:.2f}%"},
            {'Metric': '', 'Value': ''},
            {'Metric': 'PRIMARY LEDGER (LEDGER 1) SUMMARY', 'Value': ''},
            {'Metric': 'Total Ledger 1 Records', 'Value': len(ledger1_df)},
            {'Metric': 'Matched with Bank Statement', 'Value': len(matched_ledger1)},
            {'Metric': 'Unmatched with Bank Statement', 'Value': ledger1_unmatched},
            {'Metric': 'Ledger 1 Match Rate', 'Value': f"{(len(matched_ledger1)/len(ledger1_df)*100) if len(ledger1_df) > 0 else 0:.2f}%"},
            {'Metric': '', 'Value': ''},
            {'Metric': 'SECONDARY LEDGER (LEDGER 2) SUMMARY', 'Value': ''},
            {'Metric': 'Total Ledger 2 Records', 'Value': len(ledger2_df)},
            {'Metric': 'Matched with Bank Statement', 'Value': len(matched_ledger2)},
            {'Metric': 'Unmatched with Bank Statement', 'Value': ledger2_unmatched},
            {'Metric': 'Ledger 2 Match Rate', 'Value': f"{(len(matched_ledger2)/len(ledger2_df)*100) if len(ledger2_df) > 0 else 0:.2f}%"},
        ]
        summary_df = pd.DataFrame.from_records(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Prepare columns for export
        def prepare_columns(df):
            cols = [c for c in df.columns if c not in ['Status_1', 'Status_2', ' ', '  ', '   ', 
                                                         'clean_date', 'internal_amount', 'match_date', 
                                                         'match_amount', 'original_bank_index', 
                                                         'original_ledger_index']]
            if 'Status_1' in df.columns and 'Status_2' in df.columns:
                return cols + [' ', '  ', '   ', 'Status_1', ' ', '  ', '   ', 'Status_2', ' ', '  ', '   ']
            elif 'Status_1' in df.columns:
                return cols + [' ', '  ', '   ', 'Status_1', ' ', '  ', '   ']
            elif 'Status_2' in df.columns:
                return cols + [' ', '  ', '   ', 'Status_2', ' ', '  ', '   ']
            else:
                return cols + [' ', '  ', '   ']
        
        # Bank sheets
        bank_cols = prepare_columns(bank_df)
        bank_df[bank_cols].to_excel(writer, sheet_name='Bank Statement (All)', index=False)
        bank_df[bank_df['Status_1'] == 'Matched_Stage1'][bank_cols].to_excel(writer, sheet_name='Bank - Matched_Stage1', index=False)
        bank_df[bank_df['Status_1'] == 'Unmatched_Stage1'][bank_cols].to_excel(writer, sheet_name='Bank - Unmatched_Stage1', index=False)
        bank_df[bank_df['Status_2'] == 'Matched_Stage2'][bank_cols].to_excel(writer, sheet_name='Bank - Matched_Stage2', index=False)
        bank_df[bank_df['Status_2'] == 'Unmatched_Stage2'][bank_cols].to_excel(writer, sheet_name='Bank - Unmatched_Stage2', index=False)
        
        # Ledger 1 sheets
        ledger1_cols = prepare_columns(ledger1_df)
        ledger1_df[ledger1_cols].to_excel(writer, sheet_name='Ledger 1 (All)', index=False)
        ledger1_df[ledger1_df['Status_1'] == 'Matched_Stage1'][ledger1_cols].to_excel(writer, sheet_name='Ledger 1 - Matched_Stage1', index=False)
        ledger1_df[ledger1_df['Status_1'] == 'Unmatched_Stage1'][ledger1_cols].to_excel(writer, sheet_name='Ledger 1 - Unmatched_Stage1', index=False)
        
        # Ledger 2 sheets
        ledger2_cols = prepare_columns(ledger2_df)
        ledger2_df[ledger2_cols].to_excel(writer, sheet_name='Ledger 2 (All)', index=False)
        ledger2_df[ledger2_df['Status_2'] == 'Matched_Stage2'][ledger2_cols].to_excel(writer, sheet_name='Ledger 2 - Matched_Stage2', index=False)
        ledger2_df[ledger2_df['Status_2'] == 'Unmatched_Stage2'][ledger2_cols].to_excel(writer, sheet_name='Ledger 2 - Unmatched_Stage2', index=False)
    
    print("\n[SUCCESS] Results saved successfully!")
    print("\n[INFO] Output file contains:")
    print("   1. Summary - Complete reconciliation overview")
    print("   2-6. Bank Statement sheets (All, Matched_Stage1, Unmatched_Stage1, Matched_Stage2, Unmatched_Stage2)")
    print("   7-9. Ledger 1 sheets (All, Matched_Stage1, Unmatched_Stage1)")
    print("   10-12. Ledger 2 sheets (All, Matched_Stage2, Unmatched_Stage2)")
    
    print("\n" + "="*70)
    print("TWO-STAGE RECONCILIATION COMPLETE!")
    print("="*70)


def main():
    """
    Main function that reads configuration from .env file.
    """
    # Read file paths from environment variables
    BANK_FILE = os.getenv('BANK_STATEMENT_FILE_PATH', 'sample_bank_statement.xlsx')
    LEDGER1_FILE = os.getenv('LEDGER_FILE_PATH', 'sample_ledger.xlsx')
    LEDGER2_FILE = os.getenv('LEDGER2_FILE_PATH', 'sample_general_ledger.xlsx')
    OUTPUT_FILE = os.getenv('OUTPUT_FILE_PATH', 'Two_Stage_Reconciliation_Results.xlsx')
    
    # Debug print to confirm the loaded values
    print(f"DEBUG: Loaded BANK_FILE = {BANK_FILE}")
    print(f"DEBUG: Loaded LEDGER1_FILE = {LEDGER1_FILE}")
    print(f"DEBUG: Loaded LEDGER2_FILE = {LEDGER2_FILE}")
    print(f"DEBUG: Loaded OUTPUT_FILE = {OUTPUT_FILE}")
    
    # Run the two-stage reconciliation
    two_stage_reconciliation(BANK_FILE, LEDGER1_FILE, LEDGER2_FILE, OUTPUT_FILE)

if __name__ == "__main__":
    main()