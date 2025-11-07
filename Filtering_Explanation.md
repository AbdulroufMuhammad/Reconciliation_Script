## Explanation of Data Filtering Steps

This document explains the data filtering steps performed by the reconciliation script, as shown in the debug output.

### 1. Header Extraction

*   **Debug Output:** `DEBUG: After header extraction, records: 4054`
*   **Purpose:** The first step is to identify the header row in the Excel file. This is crucial because it allows the script to find and use the correct columns for "Value Date", "Credit", and "Debit" for the reconciliation process. Without correctly identifying the headers, the script cannot process the data.

### 2. Dropping Empty Rows

*   **Debug Output:** `DEBUG: After dropping empty rows, records: 4054`
*   **Purpose:** This is a data cleaning step that removes any rows that are completely empty. Empty rows contain no transactional information and can cause errors or unexpected behavior in the reconciliation logic. In this case, no empty rows were found.

### 3. Filtering Summary Rows (Currently Disabled)

*   **Debug Output:** `DEBUG: After (disabled) filtering summary rows, records: 4054`
*   **Purpose:** This filter is designed to remove summary rows from the data, such as "Total", "Grand Total", "Closing Balance", etc. These rows are not individual transactions and can cause incorrect matches if they are not removed. 
*   **Current Status:** As per your request, this filter is currently **disabled**.

### 4. Date Validation (Currently Disabled)

*   **Debug Output:** `DEBUG: After (disabled) date validation, valid dates: 4054`
*   **Purpose:** This filter is designed to ensure that only rows with valid, parseable dates in the "Value Date" or "Trans Date" column are included. This is important for date-based matching, as it prevents errors and ensures that only valid transactions are considered for reconciliation.
*   **Current Status:** As per your request, this filter is currently **disabled**.

### 5. Amount Validation

*   **Debug Output:** `DEBUG: After amount validation, valid amounts: 3501`
*   **Purpose:** This is the primary filter that is currently active. It ensures that only rows with valid amounts are included in the reconciliation process. The specific logic is:
    *   **For Bank Statements:** It keeps only the rows where the "Credit" column contains a valid, non-empty value.
    *   **For Ledgers:** It keeps only the rows where the "Debit" column contains a valid, non-empty value. It also removes any spaces from the debit column to ensure that numbers with spaces are correctly parsed.
*   **Current Status:** This filter is **enabled** and is the only active filter, as per your request.

### 6. Final Record Count

*   **Debug Output:** `DEBUG: Final records after all filters: 3501`
*   **Purpose:** This is the final count of records that will be used in the reconciliation process after all active filters have been applied. In this case, it is the result of the amount validation filter.
