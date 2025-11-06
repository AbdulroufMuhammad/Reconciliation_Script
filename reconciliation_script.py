import pandas as pd
import numpy as np
from datetime import datetime
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class ReconciliationScript:
    """
    A comprehensive bank reconciliation script that matches bank statement records 
    with ledger records and outputs a detailed Excel workbook.
    """
    
    def __init__(self, bank_file_path, ledger_file_path, output_file_path):
        """
        Initialize the reconciliation script with file paths.
        
        Args:
            bank_file_path (str): Path to the bank statement file (Excel/CSV)
            ledger_file_path (str): Path to the ledger file (Excel/CSV)
            output_file_path (str): Path for the output Excel workbook
        """
        self.bank_file_path = bank_file_path
        self.ledger_file_path = ledger_file_path
        self.output_file_path = output_file_path
        
        # Default column names
        self.bank_date_col = 'Value Date'
        self.bank_credit_col = 'Credit'
        self.bank_debit_col = 'Debit'
        self.ledger_date_col = 'Value Date'
        self.ledger_credit_col = 'Credit'
        self.ledger_debit_col = 'Debit'
        
        # DataFrames to store loaded data
        self.bank_df = None
        self.ledger_df = None
        
        # Results of reconciliation
        self.matched_bank_indices = []
        self.matched_ledger_indices = []
        self.bank_statuses = []
        self.ledger_statuses = []
        
        # Summary statistics
        self.summary_stats = {}
    
    def load_data(self):
        """
        Load bank and ledger files with support for both Excel and CSV formats.
        Handles different encodings for CSV files.
        """
        print(f"Loading Bank Statement (BASE): {self.bank_file_path}")
        
        # Load bank file
        if self.bank_file_path.lower().endswith('.xlsx') or self.bank_file_path.lower().endswith('.xls'):
            self.bank_df = pd.read_excel(self.bank_file_path)
        elif self.bank_file_path.lower().endswith('.csv'):
            # Try different encodings for CSV files
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    self.bank_df = pd.read_csv(self.bank_file_path, encoding=encoding)
                    print(f"Successfully loaded CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not load CSV file with any of the attempted encodings")
        else:
            raise ValueError(f"Unsupported file format: {self.bank_file_path}")
        
        print(f"Bank Statement shape: {self.bank_df.shape}")
        print(f"Bank Statement columns: {list(self.bank_df.columns)}")
        
        # Check if required columns exist
        if 'Value Date' not in self.bank_df.columns or ('Credit' not in self.bank_df.columns and 'Debit' not in self.bank_df.columns):
            print("Warning: Required columns ('Value Date', 'Credit'/'Debit') may be missing in bank file")
        
        print(f"\nLoading Ledger file: {self.ledger_file_path}")
        
        # Load ledger file
        if self.ledger_file_path.lower().endswith('.xlsx') or self.ledger_file_path.lower().endswith('.xls'):
            self.ledger_df = pd.read_excel(self.ledger_file_path)
        elif self.ledger_file_path.lower().endswith('.csv'):
            # Try different encodings for CSV files
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    self.ledger_df = pd.read_csv(self.ledger_file_path, encoding=encoding)
                    print(f"Successfully loaded CSV with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not load CSV file with any of the attempted encodings")
        else:
            raise ValueError(f"Unsupported file format: {self.ledger_file_path}")
        
        print(f"Ledger shape: {self.ledger_df.shape}")
        print(f"Ledger columns: {list(self.ledger_df.columns)}")
        
        # Check if required columns exist
        if 'Value Date' not in self.ledger_df.columns or ('Credit' not in self.ledger_df.columns and 'Debit' not in self.ledger_df.columns):
            print("Warning: Required columns ('Value Date', 'Credit'/'Debit') may be missing in ledger file")
    
    def clean_columns(self):
        """
        Clean column names by stripping whitespace and standardize values.
        """
        print("\nColumns cleaned.")
        
        # Clean column names by stripping whitespace
        self.bank_df.columns = self.bank_df.columns.str.strip()
        self.ledger_df.columns = self.ledger_df.columns.str.strip()
        
        # Standardize values in the dataframes
        self.bank_df = self.bank_df.apply(lambda x: x.map(self.standardize_value) if x.dtype == "object" else x)
        self.ledger_df = self.ledger_df.apply(lambda x: x.map(self.standardize_value) if x.dtype == "object" else x)
    
    def standardize_value(self, value):
        """
        Standardize individual values for comparison.
        
        Args:
            value: The value to standardize
            
        Returns:
            Standardized value
        """
        if pd.isna(value):
            return value
        
        if isinstance(value, str):
            # Remove commas and strip whitespace
            value = value.replace(',', '').strip()
            # Try to convert to float if it's a numeric string
            try:
                # Try to convert to float without changing the representation
                num_value = float(value)
                # If it's an integer value, return as int
                if num_value.is_integer():
                    return int(num_value)
                return num_value
            except ValueError:
                # If it's not numeric, return the cleaned string
                return value
        elif isinstance(value, (int, float)):
            return value
        else:
            return value
    
    def reconcile_with_status(self):
        """
        Perform matching between bank and ledger records and generate status.
        Uses Value Date and Credit/Debit amounts for matching.
        """
        print(f"\n=== Starting Reconciliation ===")
        print(f"Matching: Bank [{self.bank_date_col} + {self.bank_credit_col}] with Ledger [{self.ledger_date_col} + {self.ledger_debit_col}]")
        
        # Create copies of dataframes to avoid modifying original data
        bank_df = self.bank_df.copy()
        ledger_df = self.ledger_df.copy()
        
        # Add temporary columns for matching using dynamic column names
        # Check if the expected columns exist
        if self.bank_date_col not in bank_df.columns:
            print(f"Warning: Expected date column '{self.bank_date_col}' not found in bank file. Available columns: {list(bank_df.columns)}")
            # Try to find a date-like column (look for common date column names)
            date_cols = [col for col in bank_df.columns if any(keyword in col.lower() for keyword in ['date', 'value', 'trans', 'time', 'period'])]
            if date_cols:
                self.bank_date_col = date_cols[0]  # Use the first potential date column
                print(f"Using '{self.bank_date_col}' as date column for bank file")
            else:
                # If no date-like column found, use the first column as default
                self.bank_date_col = bank_df.columns[0]
                print(f"Using first column '{self.bank_date_col}' as date column for bank file")
        
        if self.ledger_date_col not in ledger_df.columns:
            print(f"Warning: Expected date column '{self.ledger_date_col}' not found in ledger file. Available columns: {list(ledger_df.columns)}")
            # Try to find a date-like column
            date_cols = [col for col in ledger_df.columns if any(keyword in col.lower() for keyword in ['date', 'value', 'trans', 'time', 'period'])]
            if date_cols:
                self.ledger_date_col = date_cols[0]  # Use the first potential date column
                print(f"Using '{self.ledger_date_col}' as date column for ledger file")
            else:
                # If no date-like column found, use the first column as default
                self.ledger_date_col = ledger_df.columns[0]
                print(f"Using first column '{self.ledger_date_col}' as date column for ledger file")

        if self.bank_credit_col not in bank_df.columns:
            print(f"Warning: Expected credit column '{self.bank_credit_col}' not found in bank file.")
            # Try to find a credit-like column
            credit_cols = [col for col in bank_df.columns if any(keyword in col.lower() for keyword in ['credit', 'cr', 'amount', 'value'])]
            if credit_cols:
                self.bank_credit_col = credit_cols[0]
                print(f"Using '{self.bank_credit_col}' as credit column for bank file")
            else:
                # If no credit-like column found, use the second column as default (assuming first is date)
                if len(bank_df.columns) > 1:
                    self.bank_credit_col = bank_df.columns[1]
                    print(f"Using second column '{self.bank_credit_col}' as credit column for bank file")
        
        if self.ledger_debit_col not in ledger_df.columns:
            print(f"Warning: Expected debit column '{self.ledger_debit_col}' not found in ledger file.")
            # Try to find a debit-like column
            debit_cols = [col for col in ledger_df.columns if any(keyword in col.lower() for keyword in ['debit', 'dr', 'amount', 'value'])]
            if debit_cols:
                self.ledger_debit_col = debit_cols[0]
                print(f"Using '{self.ledger_debit_col}' as debit column for ledger file")
            else:
                # If no debit-like column found, use the second column as default (assuming first is date)  
                if len(ledger_df.columns) > 1:
                    self.ledger_debit_col = ledger_df.columns[1]
                    print(f"Using second column '{self.ledger_debit_col}' as debit column for ledger file")
        
        # Add temporary columns for matching
        bank_df['temp_date'] = pd.to_datetime(bank_df[self.bank_date_col], errors='coerce')
        ledger_df['temp_date'] = pd.to_datetime(ledger_df[self.ledger_date_col], errors='coerce')
        
        # Handle both Credit and Debit columns for matching using dynamic column names
        if self.bank_credit_col in bank_df.columns and self.ledger_debit_col in ledger_df.columns:
            # Standard matching: Bank Credit with Ledger Debit
            bank_df['temp_amount'] = pd.to_numeric(bank_df[self.bank_credit_col], errors='coerce')
            ledger_df['temp_amount'] = pd.to_numeric(ledger_df[self.ledger_debit_col], errors='coerce')
            
            # Create temporary matching keys
            bank_df['temp_key'] = bank_df['temp_date'].dt.strftime('%Y-%m-%d') + '_' + bank_df['temp_amount'].astype(str)
            ledger_df['temp_key'] = ledger_df['temp_date'].dt.strftime('%Y-%m-%d') + '_' + ledger_df['temp_amount'].astype(str)
            
            # Find matches - only match non-null values
            matches = pd.merge(
                bank_df.reset_index().rename(columns={'index': 'bank_idx'}),
                ledger_df.reset_index().rename(columns={'index': 'ledger_idx'}),
                left_on='temp_key',
                right_on='temp_key',
                how='inner'
            )
            
            # Filter out rows where date or amount was null (resulted in NaT or NaN)
            matches = matches[
                (matches['temp_date_x'].notna()) & 
                (matches['temp_date_y'].notna()) & 
                (matches['temp_amount_x'].notna()) & 
                (matches['temp_amount_y'].notna())
            ]
            
            # Get matched indices
            matched_bank_indices = matches['bank_idx'].tolist()
            matched_ledger_indices = matches['ledger_idx'].tolist()
        else:
            # If columns don't exist, no matches can be found
            matched_bank_indices = []
            matched_ledger_indices = []
        
        # Create status lists based on matches
        bank_statuses = []
        ledger_statuses = []
        
        # Initialize all records as unmatched
        for i in range(len(bank_df)):
            if i in matched_bank_indices:
                bank_statuses.append("Matched with Ledger")
            else:
                bank_statuses.append("Unmatched with Ledger")
        
        for i in range(len(ledger_df)):
            if i in matched_ledger_indices:
                ledger_statuses.append("Matched with Bank")
            else:
                ledger_statuses.append("Unmatched with Bank")
        
        # Store results
        self.matched_bank_indices = matched_bank_indices
        self.matched_ledger_indices = matched_ledger_indices
        self.bank_statuses = bank_statuses
        self.ledger_statuses = ledger_statuses
        
        print(f"Finding matches...")
        print(f"Found {len(matched_bank_indices)} unique matches")
        
        # Add status columns to original dataframes
        self.bank_df['Status'] = self.bank_statuses
        self.ledger_df['Status'] = self.ledger_statuses
    
    def create_summary(self):
        """
        Create summary statistics for the reconciliation.
        """
        print("\n=== Reconciliation Summary ===")
        
        # Calculate statistics
        total_bank_records = len(self.bank_df)
        matched_bank_records = len(self.matched_bank_indices)
        unmatched_bank_records = total_bank_records - matched_bank_records
        
        total_ledger_records = len(self.ledger_df)
        matched_ledger_records = len(self.matched_ledger_indices)
        unmatched_ledger_records = total_ledger_records - matched_ledger_records
        
        # Calculate match rate
        total_possible_matches = min(total_bank_records, total_ledger_records)
        match_rate = (matched_bank_records / total_possible_matches * 100) if total_possible_matches > 0 else 0
        
        # Store statistics
        self.summary_stats = {
            'total_bank_records': total_bank_records,
            'matched_bank_records': matched_bank_records,
            'unmatched_bank_records': unmatched_bank_records,
            'total_ledger_records': total_ledger_records,
            'matched_ledger_records': matched_ledger_records,
            'unmatched_ledger_records': unmatched_ledger_records,
            'match_rate': match_rate
        }
        
        # Print summary
        print(f"BANK STATEMENT (BASE):")
        print(f"  - Total records: {total_bank_records}")
        print(f"  - Matched with Ledger: {matched_bank_records}")
        print(f"  - Unmatched with Ledger: {unmatched_bank_records}")
        
        print(f"\nLEDGER:")
        print(f"  - Total records: {total_ledger_records}")
        print(f"  - Matched with Bank: {matched_ledger_records}")
        print(f"  - Unmatched with Bank: {unmatched_ledger_records}")
        
        print(f"\nMatch Rate: {match_rate:.2f}%")
    
    def save_results(self):
        """
        Save the comprehensive workbook with all required sheets.
        """
        print(f"\nSaving results to: {self.output_file_path}")
        
        with pd.ExcelWriter(self.output_file_path, engine='openpyxl') as writer:
            # Sheet 1: Summary
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
                    self.summary_stats['total_bank_records'],
                    self.summary_stats['matched_bank_records'],
                    self.summary_stats['unmatched_bank_records'],
                    self.summary_stats['total_ledger_records'],
                    self.summary_stats['matched_ledger_records'],
                    self.summary_stats['unmatched_ledger_records'],
                    f"{self.summary_stats['match_rate']:.2f}%"
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: Bank Statement
            self.bank_df.to_excel(writer, sheet_name='Bank Statement', index=False)
            
            # Sheet 3: Bank - Matched
            bank_matched = self.bank_df[self.bank_df['Status'] == 'Matched with Ledger']
            bank_matched.to_excel(writer, sheet_name='Bank - Matched', index=False)
            
            # Sheet 4: Bank - Unmatched
            bank_unmatched = self.bank_df[self.bank_df['Status'] == 'Unmatched with Ledger']
            bank_unmatched.to_excel(writer, sheet_name='Bank - Unmatched', index=False)
            
            # Sheet 5: Ledger
            self.ledger_df.to_excel(writer, sheet_name='Ledger', index=False)
            
            # Sheet 6: Ledger - Matched
            ledger_matched = self.ledger_df[self.ledger_df['Status'] == 'Matched with Bank']
            ledger_matched.to_excel(writer, sheet_name='Ledger - Matched', index=False)
            
            # Sheet 7: Ledger - Unmatched
            ledger_unmatched = self.ledger_df[self.ledger_df['Status'] == 'Unmatched with Bank']
            ledger_unmatched.to_excel(writer, sheet_name='Ledger - Unmatched', index=False)
        
        print("Results saved successfully!")
        
        # Print output details
        print("\nOutput file contains:")
        print("  1. Summary - Overview of reconciliation")
        print("  2. Bank Statement - Full data with status")
        print("  3. Bank - Matched - Only matched records")
        print("  4. Bank - Unmatched - Only unmatched records")
        print("  5. Ledger - Full data with status")
        print("  6. Ledger - Matched - Only matched records")
        print("  7. Ledger - Unmatched - Only unmatched records")
    
    def run_reconciliation(self):
        """
        Execute the full reconciliation workflow.
        """
        print("="*60)
        print("BANK RECONCILIATION SCRIPT")
        print("="*60)
        print("BASE RECORD: Bank Statement")
        print("MATCHING: Bank [Value Date + Credit] with Ledger [Value Date + Debit]")
        print("="*60)
        
        # Load the data
        self.load_data()
        
        # Clean columns
        self.clean_columns()
        
        # Perform reconciliation
        self.reconcile_with_status()
        
        # Create summary
        self.create_summary()
        
        # Save results
        self.save_results()
        
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
    OUTPUT_FILE = os.getenv('OUTPUT_FILE_PATH', 'Reconciliation_Results.xlsx')
    
    # Read column mappings from environment variables (for files with generic column names)
    BANK_DATE_COL = os.getenv('BANK_DATE_COLUMN', 'Value Date')
    BANK_CREDIT_COL = os.getenv('BANK_CREDIT_COLUMN', 'Credit')
    BANK_DEBIT_COL = os.getenv('BANK_DEBIT_COLUMN', 'Debit')
    
    LEDGER_DATE_COL = os.getenv('LEDGER_DATE_COLUMN', 'Value Date')
    LEDGER_CREDIT_COL = os.getenv('LEDGER_CREDIT_COLUMN', 'Credit')
    LEDGER_DEBIT_COL = os.getenv('LEDGER_DEBIT_COLUMN', 'Debit')
    
    # Create and run the reconciliation script
    reconciler = ReconciliationScript(BANK_FILE, LEDGER_FILE, OUTPUT_FILE)
    reconciler.bank_date_col = BANK_DATE_COL
    reconciler.bank_credit_col = BANK_CREDIT_COL
    reconciler.bank_debit_col = BANK_DEBIT_COL
    reconciler.ledger_date_col = LEDGER_DATE_COL
    reconciler.ledger_credit_col = LEDGER_CREDIT_COL
    reconciler.ledger_debit_col = LEDGER_DEBIT_COL
    
    reconciler.run_reconciliation()


if __name__ == "__main__":
    main()