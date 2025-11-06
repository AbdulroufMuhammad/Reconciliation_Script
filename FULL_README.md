# Bank Reconciliation Script

This Python script provides comprehensive functionality for reconciling bank records between two data files (Excel or CSV).

## Features

- **Multi-Format Support**: Works with both Excel (.xlsx, .xls) and CSV (.csv) files
- **Header Validation**: Checks if column headers match between files before processing
- **Fuzzy Header Matching**: Provides suggestions when headers don't match exactly
- **Value Standardization**: Normalizes values for comparison while preserving decimal precision
- **Record Matching**: Finds matching records based on common columns
- **Unmatched Record Identification**: Identifies records present in one file but not the other
- **Flexible Output**: Saves results to Excel files with multiple sheets if needed

## Requirements

- Python 3.7+
- Required packages: `pandas`, `openpyxl`

Install requirements with:
```bash
pip install -r requirements.txt
```

## Usage

### Command Line Interface

```bash
python reconciliation_script.py --bank-file path/to/bank_file.xlsx --reference-file path/to/reference_file.xlsx --output-file output.xlsx
```

Options:
- `--bank-file`: Path to the bank transfer file (required) - supports .xlsx, .xls, .csv
- `--reference-file`: Path to the reference file (required) - supports .xlsx, .xls, .csv  
- `--output-file`: Path for the output file (optional, defaults to "Matched_Results.xlsx")
- `--columns`: Specific column names to match on (optional, space-separated)
- `--no-header-check`: Skip header matching check (by default, headers must match)

### Example Usage

#### Basic reconciliation (requires matching headers):
```bash
python reconciliation_script.py --bank-file bank_data.xlsx --reference-file gtb_data.xlsx
```

#### Reconciliation without header checking:
```bash
python reconciliation_script.py --bank-file bank_data.csv --reference-file gtb_data.xlsx --no-header-check
```

#### Reconciliation with specific columns:
```bash
python reconciliation_script.py --bank-file bank_data.xlsx --reference-file gtb_data.xlsx --columns Transaction_ID Amount Date
```

### Programmatic Usage

```python
from reconciliation_script import ReconciliationScript

# Initialize the reconciler
reconciler = ReconciliationScript(
    bank_file_path='bank_data.csv',
    reference_file_path='reference_data.xlsx',
    output_path='matched_results.xlsx'
)

# Run reconciliation with header checking (default)
success = reconciler.run_reconciliation(require_matching_headers=True)

if success:
    print("Reconciliation completed successfully!")
    
    # You can also access unmatched records
    unmatched_bank, unmatched_ref = reconciler.find_unmatched_records()
    
    # Save detailed results (matched + unmatched) to separate sheets
    reconciler.save_all_results('detailed_results.xlsx')
```

## How Reconciliation Works

1. **File Loading**: Loads both files (Excel or CSV) into pandas DataFrames
2. **Header Validation**: Checks if column headers match between the files
3. **Data Standardization**: 
   - Strips whitespace from headers
   - Standardizes values (handles numeric values to preserve decimal precision)
   - Treats '12' and '12.00' as equivalent values
4. **Record Matching**: Finds records that match on common columns
5. **Result Output**: Saves matched records to the specified output file

## Supported File Formats

- Excel files: .xlsx, .xls
- Text files: .csv

The output is always in Excel format (.xlsx) to support multiple sheets if needed.

## Important Notes

- The script will alert you if column headers don't match
- When headers don't match, you can either:
  1. Correct the headers to match before running reconciliation
  2. Use the `--no-header-check` flag to proceed anyway
- Decimal precision is preserved during value comparison
- Values like '12' and '12.00' are treated as equivalent
- The script identifies both matched and unmatched records for comprehensive reconciliation