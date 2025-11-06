# Bank Reconciliation Script - Matching Process Explanation

## Overview
This document explains how the bank reconciliation script matches transactions between two datasets (Bank and Reference files) based on Value Date and Amount.

## Key Features of the Matching Algorithm

### 1. Data Standardization
Before matching, the script standardizes the data to ensure accurate comparisons:

- **Numeric Formatting**: All amounts are converted to standard numeric format, with commas removed (e.g., '10,000.00' becomes '10000.00')
- **Date Formatting**: Both files use consistent date formats
- **Case Insensitivity**: Text comparisons are handled case-insensitively 
- **Whitespace Handling**: Leading and trailing whitespace is removed

### 2. Matching Process

#### Step 1: Initial Merge Operation
The script first performs a full merge between both datasets:
```
all_possible_matches = pd.merge(bank_df, reference_df, on=['Value Date', 'Amount'], how="inner")
```

This creates all possible matches where Value Date and Amount are identical between the files.

#### Step 2: Duplicate Elimination
To prevent the duplication issue (where multiple identical records create Cartesian products), the script implements a unique matching approach:

1. Each transaction in either file can only be matched **once**
2. The algorithm tracks already matched records using sets:
   - `matched_bank_indices`: Tracks which Bank records have been matched
   - `matched_reference_indices`: Tracks which Reference records have been matched

3. The algorithm processes each possible match and only accepts it if both records are still unmatched:
   ```
   if bank_orig_idx not in matched_bank_indices and ref_orig_idx not in matched_reference_indices:
       # Accept this match and add both indices to matched sets
   ```

### 3. Example Scenarios

#### Scenario A: One-to-One Match
- Bank File: 1 transaction on 2024-01-02 with Amount 10,000.00
- Reference File: 1 transaction on 2024-01-02 with Amount 10,000.00
- **Result**: 1 match recorded

#### Scenario B: Multiple in Bank, Single in Reference
- Bank File: 3 transactions on 2024-01-02 with Amount 10,000.00 each
- Reference File: 1 transaction on 2024-01-02 with Amount 10,000.00
- **Result**: 1 match recorded (only one of the Bank transactions gets matched)
- Remaining 2 Bank transactions appear as "unmatched"

#### Scenario C: Single in Bank, Multiple in Reference
- Bank File: 1 transaction on 2024-01-02 with Amount 10,000.00
- Reference File: 4 transactions on 2024-01-02 with Amount 10,000.00
- **Result**: 1 match recorded (only one of the Reference transactions gets matched)
- Remaining 3 Reference transactions appear as "unmatched"

## Output Structure

The script generates a comprehensive Excel workbook with multiple sheets:

### Sheet 1: `00_Summary`
- Provides summary statistics
- Total records in each file
- Match rate percentage
- Counts of different transaction types

### Sheet 2: `01_Matches`
- Contains all matched transactions
- Columns: Value Date, Amount, Status, Source
- These transactions exist in both files with identical dates and amounts

### Sheet 3: `02_Unmatched_Bank`
- Transactions that appear only in the Bank file
- No matching transactions found in the Reference file
- Could indicate pending transactions, recording errors, or timing differences

### Sheet 4: `03_Unmatched_Reference`
- Transactions that appear only in the Reference file
- No matching transactions found in the Bank file
- Could indicate pending transactions, recording errors, or timing differences

### Sheet 5: `Comprehensive_Report`
- Contains all data with status indicators
- Used for internal processing but available for detailed analysis

## Key Benefits of This Approach

1. **Prevents Duplication**: Ensures each transaction is only matched once, preventing artificial inflation of matches
2. **Accurate Reconciliation**: Provides a realistic view of actually matching transactions
3. **Transparency**: Clear separation between matches and mismatches
4. **Comprehensive Logging**: Every search operation is logged for traceability
5. **Professional Format**: Organized in a single workbook for easy review during presentations

## Technical Implementation Notes

- The script uses pandas merge operations with 'inner' joins to find matches
- Original indices are preserved to maintain traceability to source records
- Memory-efficient processing through iterative algorithms rather than storing large intermediate datasets
- Handles both string and numeric formats consistently
- Robust error handling for malformed data

This methodology ensures accurate bank reconciliation while maintaining audit trails and enabling efficient review processes.