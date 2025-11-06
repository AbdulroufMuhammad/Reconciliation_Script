# Reconciliation Script

This Python script provides a comprehensive solution for reconciling two Excel files by matching records based on common columns.

## Features

- Loads two Excel or CSV files for comparison
- Checks if column headers match before proceeding (configurable)
- Performs fuzzy matching of headers if they don't match exactly
- Standardizes values for comparison while preserving decimal precision
- Finds matching records between the files
- Identifies unmatched records in both files
- Saves results to Excel files

## Requirements

- Python 3.7+
- pandas
- openpyxl (for Excel file support)

Install with: `pip install -r requirements.txt`

## Installation

To install the required packages:

```bash
pip install pandas openpyxl
```

## Usage

### Environment File Configuration (.env)

The script supports configuration via a .env file. Create a .env file in the project root with the following format:

```
# File paths for bank reconciliation
BANK_FILE_PATH=Test1/test/TestFile1.xlsx
REFERENCE_FILE_PATH=Test1/test/TestFile2.xlsx
OUTPUT_FILE_PATH=Matched_Results.xlsx
```

### Command Line Usage

```bash
python reconciliation_script.py --bank-file <path> --reference-file <path> [options]
```

Options:
- `--bank-file`: Path to the bank transfer file (overrides .env value)
- `--reference-file`: Path to the reference file (overrides .env value) 
- `--output-file`: Path for the output file (overrides .env value)
- `--columns`: Specific column names to match on (optional, space-separated)
- `--no-header-check`: Skip header matching check (by default, headers must match)

### Using Your Specific Dataset

For your specific dataset files:

**Run with .env configuration:**
```bash
python reconciliation_script.py
```

**Excel files with command line override:**
```bash
python reconciliation_script.py --bank-file Test1/NEWGTBNAIRA.xlsx --reference-file Test1/GTBNAIRA.xlsx --output-file Matched_GTBNaira.xlsx
```

**CSV files with command line override:**
```bash
python reconciliation_script.py --bank-file DATA/NEWGTBNAIRA.csv --reference-file DATA/GTBNAIRA.csv --output-file Matched_GTBNaira_CSV.xlsx
```

### Pre-configured Scripts for Your Dataset

We've provided specific scripts for your dataset:

1. **run_dataset_reconciliation.py** - Works with Excel files
2. **run_csv_dataset_reconciliation.py** - Works with CSV files

Run with:
```bash
python run_dataset_reconciliation.py
```
or
```bash
python run_csv_dataset_reconciliation.py
```

### Example Command Line Usage

```bash
python reconciliation_script.py --bank-file NEWGTBNAIRA.xlsx --reference-file GTBNAIRA.xlsx --output-file Matched_GTBNaira.xlsx
```

### Programmatic Usage

```python
from reconciliation_script import ReconciliationScript

# Create an instance
reconciler = ReconciliationScript(
    bank_file_path="NEWGTBNAIRA.xlsx",
    reference_file_path="GTBNAIRA.xlsx", 
    output_path="Matched_GTBNaira.xlsx"
)

# Run the basic reconciliation
reconciler.run_reconciliation()

# Or run step by step for more control
reconciler.load_data()
reconciler.clean_columns()
common_cols = reconciler.find_common_columns()
reconciler.find_matches(common_cols)
reconciler.save_matched_results()
```

## How It Works

1. **Load Data**: The script loads both Excel files into pandas DataFrames
2. **Clean Data**: Column names are stripped of whitespace and all data is converted to strings for uniform comparison
3. **Find Common Columns**: Identifies columns that exist in both files
4. **Match Records**: Uses pandas merge with 'inner' join to find records that match on all common columns
5. **Save Results**: Saves matched records to an Excel file

## Advanced Features

The script also provides methods to:
- Find unmatched records in both files
- Save all results (matched and unmatched) to different sheets in a single Excel file
- Specify specific columns to match on instead of using all common columns

## Output Files

- `Matched_Results.xlsx`: Contains only the records that matched between both files
- When using `save_all_results()`: Creates a file with sheets for matched, unmatched in bank file, and unmatched in reference file