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

def find_actual_data_rows(df, file_type):
    """
    Find the actual transaction data rows by identifying the header row and filtering out 
    non-transaction rows like summaries, totals, etc.
    """
    # Find header rows by looking for "Value Date", "Credit", "Debit" keywords
    header_row = None
    for i in range(min(20, len(df))):  # Check first 20 rows for headers
        row_values = df.iloc[i].astype(str)
        if file_type == "bank":
            if any('value date' in val.lower() or 'trans date' in val.lower() for val in row_values) and \
               any('credit' in val.lower() or 'debit' in val.lower() for val in row_values):
                header_row = i
                break
        elif file_type == "ledger":
            if any('value date' in val.lower() or 'trans date' in val.lower() for val in row_values) and \
               any('debit' in val.lower() for val in row_values):
                header_row = i
                break

    # If exact headers not found, try to find any date/credit or date/debit combination
    if header_row is None:
        for i in range(min(20, len(df))):
            row_values = df.iloc[i].astype(str)
            has_date = any('date' in val.lower() and 'value' in val.lower() for val in row_values)
            if file_type == "bank":
                has_amount = any('credit' in val.lower() or 'debit' in val.lower() for val in row_values)
            elif file_type == "ledger":
                has_amount = any('debit' in val.lower() for val in row_values)
            if has_date and has_amount:
                header_row = i
                break

    if header_row is not None:
        # Set headers and get data after header row
        df_with_headers = df.iloc[header_row:].copy()
        df_with_headers.columns = df_with_headers.iloc[0]
        df_data = df_with_headers.iloc[1:].reset_index(drop=True)
        
        # Filter out empty rows
        df_data = df_data.dropna(how='all')
        
        # Filter out rows that are likely summaries or totals
        summary_keywords = ['total', 'grand total', 'sub total', 'summary', 'closing balance', 'opening balance', 
                           'balance c/f', 'balance b/f', 'overall total']
        non_summary_mask = pd.Series([True] * len(df_data), index=df_data.index)
        
        for idx, row in df_data.iterrows():
            row_str = ' '.join(str(val) for val in row.values if pd.notna(val))
            row_str_lower = row_str.lower()
            
            is_summary = False
            for keyword in summary_keywords:
                if keyword in row_str_lower:
                    if keyword in ['total', 'grand total', 'sub total', 'summary', 'overall total']:
                        clean_row_str = ' '.join(row_str_lower.split())
                        if len(clean_row_str) < 50 or keyword in clean_row_str.split():
                            is_summary = True
                            break
                    elif keyword in ['closing balance', 'opening balance', 'balance c/f', 'balance b/f']:
                        is_summary = True
                        break
            
            if is_summary:
                non_summary_mask[idx] = False
        
        # Find date column
        date_col = None
        for col in df_data.columns:
            if 'value date' in str(col).lower() or 'trans date' in str(col).lower() or 'date' in str(col).lower():
                date_col = col
                break
        
        # Find amount columns
        amount_cols = []
        for col in df_data.columns:
            if file_type == "bank" and ('credit' in str(col).lower() or 'debit' in str(col).lower()):
                amount_cols.append(col)
            elif file_type == "ledger" and 'debit' in str(col).lower():
                amount_cols.append(col)
        
        # Apply date and amount validation
        if date_col:
            date_series = pd.to_datetime(df_data[date_col].astype(str), errors='coerce')
            valid_date_mask = date_series.notna()
            
            valid_amount_mask = pd.Series([True] * len(df_data), index=df_data.index)
            
            if file_type == "bank":
                credit_col = None
                for col in df_data.columns:
                    if 'credit' in str(col).lower():
                        credit_col = col
                        break
                if credit_col:
                    credit_series = pd.to_numeric(df_data[credit_col].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce')
                    original_credit_values = df_data[credit_col].astype(str).str.strip()
                    non_empty_and_non_zero_credit = (credit_series.notna()) & (credit_series != 0) & (original_credit_values != '') & (original_credit_values != 'nan') & (original_credit_values != 'NaN')
                    valid_amount_mask = non_empty_and_non_zero_credit
                else:
                    valid_amount_mask = pd.Series([False] * len(df_data), index=df_data.index)
            else:  # file_type == "ledger"
                valid_amount_mask = pd.Series([False] * len(df_data), index=df_data.index)
                for amount_col in amount_cols:
                    amount_series = pd.to_numeric(df_data[amount_col].astype(str).str.replace(',', '').str.replace(' ', ''), errors='coerce')
                    original_values = df_data[amount_col].astype(str).str.strip()
                    non_empty_and_non_zero = (amount_series.notna()) & (amount_series != 0) & (original_values != '') & (original_values != 'nan') & (original_values != 'NaN')
                    valid_amount_mask = valid_amount_mask | non_empty_and_non_zero
        else:
            valid_date_mask = pd.Series([True] * len(df_data), index=df_data.index)
            valid_amount_mask = pd.Series([False] * len(df_data), index=df_data.index)
        
        final_mask = non_summary_mask & valid_date_mask & valid_amount_mask
        
        return df_data[final_mask], header_row
    else:
        return df, 0


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
    
    # Convert amount columns to numeric
    bank_work['internal_amount'] = pd.to_numeric(bank_work[bank_credit_col].astype(str).str.replace(',', ''), errors='coerce')
    ledger_work['internal_amount'] = pd.to_numeric(ledger_work[ledger_debit_col].astype(str).str.replace(',', ''), errors='coerce')
    
    # Create match keys
    bank_work['match_date'] = bank_work['clean_date'].dt.strftime('%Y-%m-%d')
    ledger_work['match_date'] = ledger_work['clean_date'].dt.strftime('%Y-%m-%d')
    
    bank_work['match_amount'] = bank_work['internal_amount'].abs().round(2)
    ledger_work['match_amount'] = ledger_work['internal_amount'].abs().round(2)
    
    bank_work['match_date'] = bank_work['match_date'].astype(str)
    ledger_work['match_date'] = ledger_work['match_date'].astype(str)
    
    # Store original indices as a column (these are the actual dataframe indices)
    bank_work['original_bank_index'] = bank_work.index
    ledger_work['original_ledger_index'] = ledger_work.index
    
    # Create temporary DataFrames for matching
    bank_temp = bank_work[['match_date', 'match_amount', 'original_bank_index']].copy()
    ledger_temp = ledger_work[['match_date', 'match_amount', 'original_ledger_index']].copy()
    
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
    
    # Handle column names - check which suffix was actually applied
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
    print(f"\n📁 Loading Bank Statement: {bank_file}")
    if bank_file.lower().endswith('.xlsx') or bank_file.lower().endswith('.xls'):
        bank_df_raw = pd.read_excel(bank_file, header=None)
    else:
        bank_df_raw = pd.read_csv(bank_file, header=None)
    
    print(f"📁 Loading Primary Ledger: {ledger1_file}")
    if ledger1_file.lower().endswith('.xlsx') or ledger1_file.lower().endswith('.xls'):
        ledger1_df_raw = pd.read_excel(ledger1_file, header=None)
    else:
        ledger1_df_raw = pd.read_csv(ledger1_file, header=None)
    
    print(f"📁 Loading Secondary/General Ledger: {ledger2_file}")
    if ledger2_file.lower().endswith('.xlsx') or ledger2_file.lower().endswith('.xls'):
        ledger2_df_raw = pd.read_excel(ledger2_file, header=None)
    else:
        ledger2_df_raw = pd.read_csv(ledger2_file, header=None)
    
    # ========== EXTRACT TRANSACTION DATA ==========
    print("\n🔍 Extracting transaction data...")
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
        return
    
    # ========== STAGE 1: BANK vs LEDGER 1 ==========
    print("\n" + "="*70)
    print("STAGE 1: Matching Bank Statement with Primary Ledger")
    print("="*70)
    
    # Perform stage 1 matching
    matched_bank_stage1, matched_ledger1 = perform_matching(
        bank_df, ledger1_df,
        bank_date_col, bank_credit_col,
        ledger1_date_col, ledger1_debit_col,
        stage_number=1
    )
    
    # Initialize Status_1 for all dataframes
    bank_df['Status_1'] = 'Unmatched_1'
    bank_df.loc[matched_bank_stage1, 'Status_1'] = 'Matched_1'
    
    ledger1_df['Status_1'] = 'Unmatched_1'
    ledger1_df.loc[matched_ledger1, 'Status_1'] = 'Matched_1'
    
    # Stage 1 results
    total_bank = len(bank_df)
    matched_stage1_count = len(matched_bank_stage1)
    unmatched_stage1_count = total_bank - matched_stage1_count
    
    print(f"\n✅ Stage 1 Results:")
    print(f"   Total Bank records: {total_bank}")
    print(f"   Matched with Ledger 1: {matched_stage1_count}")
    print(f"   Unmatched (going to Stage 2): {unmatched_stage1_count}")
    
    # ========== STAGE 2: UNMATCHED BANK vs LEDGER 2 ==========
    print("\n" + "="*70)
    print("STAGE 2: Matching Unmatched Bank Records with Secondary Ledger")
    print("="*70)
    
    # Get only unmatched bank records from stage 1 (Status_1 = 'Unmatched_1')
    bank_unmatched_stage1 = bank_df[bank_df['Status_1'] == 'Unmatched_1'].copy()
    
    # Initialize Status_2 for bank
    bank_df['Status_2'] = ''
    
    if len(bank_unmatched_stage1) == 0:
        print("✅ All bank records matched in Stage 1. No Stage 2 needed.")
        matched_stage2_count = 0
        unmatched_stage2_count = 0
        matched_ledger2 = []
        ledger2_df['Status_2'] = ''
    else:
        # Perform stage 2 matching - only with unmatched records from stage 1
        matched_bank_stage2_indices, matched_ledger2 = perform_matching(
            bank_unmatched_stage1, ledger2_df,
            bank_date_col, bank_credit_col,
            ledger2_date_col, ledger2_debit_col,
            stage_number=2
        )
        
        # Update Status_2 for bank records
        # For matched records in stage 1, Status_2 remains empty
        # For unmatched records from stage 1, set Status_2
        bank_df.loc[bank_df['Status_1'] == 'Unmatched_1', 'Status_2'] = 'Unmatched_2'
        if matched_bank_stage2_indices:
            bank_df.loc[matched_bank_stage2_indices, 'Status_2'] = 'Matched_2'
        
        # Update Status_2 for ledger 2
        ledger2_df['Status_2'] = 'Unmatched_2'
        if matched_ledger2:
            ledger2_df.loc[matched_ledger2, 'Status_2'] = 'Matched_2'
        
        # Stage 2 results
        matched_stage2_count = len(matched_bank_stage2_indices)
        unmatched_stage2_count = unmatched_stage1_count - matched_stage2_count
        
        print(f"\n✅ Stage 2 Results:")
        print(f"   Unmatched from Stage 1: {unmatched_stage1_count}")
        print(f"   Matched with Ledger 2: {matched_stage2_count}")
        print(f"   Still Unmatched: {unmatched_stage2_count}")
    
    # ========== FINAL SUMMARY ==========
    print("\n" + "="*70)
    print("FINAL RECONCILIATION SUMMARY")
    print("="*70)
    print(f"\n📊 BANK STATEMENT:")
    print(f"   Total records: {total_bank}")
    print(f"   ├─ Matched in Stage 1 (Ledger 1): {matched_stage1_count} ({matched_stage1_count/total_bank*100:.1f}%)")
    print(f"   ├─ Matched in Stage 2 (Ledger 2): {matched_stage2_count} ({matched_stage2_count/total_bank*100:.1f}%)")
    print(f"   └─ Still Unmatched: {unmatched_stage2_count} ({unmatched_stage2_count/total_bank*100:.1f}%)")
    print(f"\n   Overall Match Rate: {(matched_stage1_count + matched_stage2_count)/total_bank*100:.1f}%")
    
    print(f"\n📊 LEDGER 1 (Primary):")
    print(f"   Total records: {len(ledger1_df)}")
    print(f"   ├─ Matched with Bank: {len(matched_ledger1)}")
    print(f"   └─ Unmatched: {len(ledger1_df) - len(matched_ledger1)}")
    
    print(f"\n📊 LEDGER 2 (Secondary/General):")
    print(f"   Total records: {len(ledger2_df)}")
    print(f"   ├─ Matched with Bank: {len(matched_ledger2)}")
    print(f"   └─ Unmatched: {len(ledger2_df) - len(matched_ledger2)}")
    
    # ========== SAVE RESULTS ==========
    print(f"\n💾 Saving results to: {output_file}")
    
    # Add gap columns for visual separation
    for df in [bank_df, ledger1_df, ledger2_df]:
        df['Gap1'] = ''
        df['Gap2'] = ''
        df['Gap3'] = ''
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet with complete information
        ledger1_unmatched = len(ledger1_df) - len(matched_ledger1)
        ledger2_unmatched = len(ledger2_df) - len(matched_ledger2)
        
        summary_data = {
            'Metric': [
                '═══════════════════════════════════════════════════',
                '                 BANK STATEMENT SUMMARY',
                '═══════════════════════════════════════════════════',
                '',
                'Total Bank Statement Records',
                '',
                '--- STAGE 1: Matching with Primary Ledger ---',
                'Matched with Ledger 1',
                'Unmatched with Ledger 1',
                'Stage 1 Match Rate',
                '',
                '--- STAGE 2: Matching Unmatched with Secondary Ledger ---',
                'Matched with Ledger 2',
                'Still Unmatched after Stage 2',
                'Stage 2 Match Rate (of unmatched)',
                '',
                '--- OVERALL BANK RECONCILIATION ---',
                'Total Matched (Stage 1 + Stage 2)',
                'Total Unmatched',
                'Overall Match Rate',
                '',
                '',
                '═══════════════════════════════════════════════════',
                '            PRIMARY LEDGER (LEDGER 1) SUMMARY',
                '═══════════════════════════════════════════════════',
                '',
                'Total Ledger 1 Records',
                '',
                '--- STAGE 1: Matching with Bank Statement ---',
                'Matched with Bank Statement',
                'Unmatched with Bank Statement',
                'Ledger 1 Match Rate',
                '',
                '',
                '═══════════════════════════════════════════════════',
                '          SECONDARY LEDGER (LEDGER 2) SUMMARY',
                '═══════════════════════════════════════════════════',
                '',
                'Total Ledger 2 Records',
                '',
                '--- STAGE 2: Matching with Unmatched Bank Records ---',
                'Matched with Bank Statement',
                'Unmatched with Bank Statement',
                'Ledger 2 Match Rate',
                '',
            ],
            'Value': [
                '',
                '',
                '',
                '',
                total_bank,
                '',
                '',
                matched_stage1_count,
                unmatched_stage1_count,
                f"{(matched_stage1_count/total_bank*100) if total_bank > 0 else 0:.2f}%",
                '',
                '',
                matched_stage2_count,
                unmatched_stage2_count,
                f"{(matched_stage2_count/unmatched_stage1_count*100) if unmatched_stage1_count > 0 else 0:.2f}%",
                '',
                '',
                matched_stage1_count + matched_stage2_count,
                unmatched_stage2_count,
                f"{((matched_stage1_count + matched_stage2_count)/total_bank*100) if total_bank > 0 else 0:.2f}%",
                '',
                '',
                '',
                '',
                '',
                '',
                len(ledger1_df),
                '',
                '',
                len(matched_ledger1),
                ledger1_unmatched,
                f"{(len(matched_ledger1)/len(ledger1_df)*100) if len(ledger1_df) > 0 else 0:.2f}%",
                '',
                '',
                '',
                '',
                '',
                '',
                len(ledger2_df),
                '',
                '',
                len(matched_ledger2),
                ledger2_unmatched,
                f"{(len(matched_ledger2)/len(ledger2_df)*100) if len(ledger2_df) > 0 else 0:.2f}%",
                '',
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Prepare columns for export (Status_1 and Status_2 at the end with Gap in between)
        def prepare_columns(df):
            cols = [c for c in df.columns if c not in ['Status_1', 'Status_2', 'Gap1', 'Gap2', 'Gap3', 'clean_date', 'internal_amount', 'match_date', 'match_amount', 'original_bank_index', 'original_ledger_index']]
            if 'Status_1' in df.columns and 'Status_2' in df.columns:
                return cols + ['Gap1', 'Status_1', 'Gap2', 'Status_2', 'Gap3']
            elif 'Status_1' in df.columns:
                return cols + ['Gap1', 'Gap2', 'Gap3', 'Status_1']
            elif 'Status_2' in df.columns:
                return cols + ['Gap1', 'Gap2', 'Gap3', 'Status_2']
            else:
                return cols + ['Gap1', 'Gap2', 'Gap3']
        
        # Bank sheets
        bank_cols = prepare_columns(bank_df)
        bank_df[bank_cols].to_excel(writer, sheet_name='Bank Statement (All)', index=False)
        bank_df[bank_df['Status_1'] == 'Matched_1'][bank_cols].to_excel(writer, sheet_name='Bank - Matched_1', index=False)
        bank_df[bank_df['Status_2'] == 'Matched_2'][bank_cols].to_excel(writer, sheet_name='Bank - Matched_2', index=False)
        bank_df[bank_df['Status_2'] == 'Unmatched_2'][bank_cols].to_excel(writer, sheet_name='Bank - Unmatched_2', index=False)
        
        # Ledger 1 sheets
        ledger1_cols = prepare_columns(ledger1_df)
        ledger1_df[ledger1_cols].to_excel(writer, sheet_name='Ledger 1 (All)', index=False)
        ledger1_df[ledger1_df['Status_1'] == 'Matched_1'][ledger1_cols].to_excel(writer, sheet_name='Ledger 1 - Matched_1', index=False)
        ledger1_df[ledger1_df['Status_1'] == 'Unmatched_1'][ledger1_cols].to_excel(writer, sheet_name='Ledger 1 - Unmatched_1', index=False)
        
        # Ledger 2 sheets
        ledger2_cols = prepare_columns(ledger2_df)
        ledger2_df[ledger2_cols].to_excel(writer, sheet_name='Ledger 2 (All)', index=False)
        ledger2_df[ledger2_df['Status_2'] == 'Matched_2'][ledger2_cols].to_excel(writer, sheet_name='Ledger 2 - Matched_2', index=False)
        ledger2_df[ledger2_df['Status_2'] == 'Unmatched_2'][ledger2_cols].to_excel(writer, sheet_name='Ledger 2 - Unmatched_2', index=False)
    
    print("\n✅ Results saved successfully!")
    print("\n📋 Output file contains:")
    print("   1. Summary - Complete reconciliation overview")
    print("   2-5. Bank Statement sheets (All, Matched_1, Matched_2, Unmatched_2)")
    print("   6-8. Ledger 1 sheets (All, Matched_1, Unmatched_1)")
    print("   9-11. Ledger 2 sheets (All, Matched_2, Unmatched_2)")
    
    print("\n" + "="*70)
    print("TWO-STAGE RECONCILIATION COMPLETE! 🎉")
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
    
    # Run the two-stage reconciliation
    two_stage_reconciliation(BANK_FILE, LEDGER1_FILE, LEDGER2_FILE, OUTPUT_FILE)

if __name__ == "__main__":
    main()