import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv

def find_actual_data_rows(df):
    """
    Find the actual data rows by identifying the header row and filtering out 
    non-transaction rows like summaries, totals, etc.
    """
    # Look for header row by searching for key column names like "Value Date" and "Credit/Debit"
    header_row = None
    for i in range(min(20, len(df))):  # Check first 20 rows for headers
        row_values = df.iloc[i].astype(str)
        if any('value date' in val.lower() for val in row_values) and \
           any('credit' in val.lower() or 'debit' in val.lower() for val in row_values):
            header_row = i
            break
    
    # If exact headers not found, try to find any date/credit or date/debit combination
    if header_row is None:
        for i in range(min(20, len(df))):
            row_values = df.iloc[i].astype(str)
            has_date = any('date' in val.lower() and 'value' in val.lower() for val in row_values)
            has_amount = any('credit' in val.lower() or 'debit' in val.lower() or 'amount' in val.lower() for val in row_values)
            if has_date and has_amount:
                header_row = i
                break

    if header_row is not None:
        # Set headers and get data after header row
        df_with_headers = df.iloc[header_row:].copy()
        df_with_headers.columns = df_with_headers.iloc[0]  # Set first row as header
        df_data = df_with_headers.iloc[1:].reset_index(drop=True)  # Get data after header
        
        # Filter out empty rows
        df_data = df_data.dropna(how='all')
        
        # Filter out rows that don't have actual transaction data
        # Look for rows with actual date values and amounts
        
        # Find date column and amount column if possible
        date_col = None
        amount_col = None
        
        for col in df_data.columns:
            if str(col).strip().lower() in ['value date', 'date', 'transaction date', 'value_date', 'txn date']:
                date_col = col
                break
        
        # If not found in headers, search for it in the first few data rows
        if date_col is None:
            for col in df_data.columns:
                try:
                    # Check first few rows to see if column contains date values
                    sample_data = df_data[col].iloc[:5].dropna()
                    if len(sample_data) > 0:
                        for val in sample_data:
                            if pd.to_datetime(str(val), errors='coerce') != pd.NaT:
                                date_col = col
                                break
                        if date_col:
                            break
                except:
                    continue
        
        # Find amount column (Credit, Debit, Amount, etc.)
        for col in df_data.columns:
            if str(col).strip().lower() in ['credit', 'debit', 'amount', 'cr', 'dr', 'value']:
                amount_col = col
                break
        
        # If we found both date and amount columns, filter rows that have valid data in both
        if date_col is not None and amount_col is not None:
            # Create a mask for valid records
            date_valid = pd.to_datetime(df_data[date_col].astype(str), errors='coerce').notna()
            amount_valid = pd.to_numeric(df_data[amount_col].astype(str).str.replace(',', ''), errors='coerce').notna()
            
            # Keep only rows where both date and amount are valid
            valid_records = df_data[date_valid & amount_valid]
            return len(valid_records), header_row
        else:
            # If we can't identify specific date/amount columns, just return non-empty rows
            return len(df_data), header_row
    else:
        # If no header row found, return total rows in the dataframe
        return len(df), 0

def count_records_in_file(file_path, file_type="unknown"):
    """
    Count the actual transaction records in an Excel file excluding headers and non-transaction rows.
    """
    print(f"Analyzing file: {file_path}")
    
    # Load the file
    if file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.xls'):
        df = pd.read_excel(file_path, header=None)
    else:
        df = pd.read_csv(file_path, header=None)
    
    print(f"Raw shape: {df.shape}")
    
    # Count actual data rows
    actual_records, header_row_idx = find_actual_data_rows(df)
    
    if header_row_idx is not None:
        print(f"Header row found at index: {header_row_idx}")
    else:
        print("No header row found")
    
    print(f"Actual transaction records found: {actual_records}")
    print("-" * 50)
    
    return actual_records

def main():
    """
    Main function to count records in files specified in .env or default files.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Read file paths from environment variables
    BANK_FILE = os.getenv('BANK_STATEMENT_FILE_PATH', 'sample_bank_statement.xlsx')
    LEDGER_FILE = os.getenv('LEDGER_FILE_PATH', 'sample_ledger.xlsx')
    
    print("="*50)
    print("ACCURATE RECORD COUNTING TOOL")
    print("="*50)
    
    # Count records in bank file
    bank_records = count_records_in_file(BANK_FILE, "bank")
    
    # Count records in ledger file
    ledger_records = count_records_in_file(LEDGER_FILE, "ledger")
    
    print("\nFinal Summary:")
    print(f"Bank Statement actual transaction records: {bank_records}")
    print(f"Ledger actual transaction records: {ledger_records}")
    
    print("\n" + "="*50)
    print("COUNTING COMPLETE!")
    print("="*50)

if __name__ == "__main__":
    main()