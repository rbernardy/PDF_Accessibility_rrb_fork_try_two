#!/bin/bash
#
# Export failure records from pdf-failure-records DynamoDB table to Excel
# Creates two sheets:
#   1. All Records - all failure records with failure_date and pdf_key
#   2. Summary - unique pdf_key values with failure counts
#
# Usage: ./bin/get-all-failure-records-from-the-pdf-failure-records-dynamodb-table.sh [substring]
#
# Example: ./bin/get-all-failure-records-from-the-pdf-failure-records-dynamodb-table.sh alaskan_caver
#

SUBSTRING="${1:-alaskan_caver}"
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
FILENAME="${SUBSTRING}-failures-${TIMESTAMP}.xlsx"

aws dynamodb scan \
  --table-name pdf-failure-records \
  --filter-expression "contains(pdf_key, :substring)" \
  --expression-attribute-values "{\":substring\": {\"S\": \"${SUBSTRING}\"}}" \
  --projection-expression "failure_date, pdf_key" \
  --output json | jq -r '["failure_date","pdf_key"], (.Items[] | [.failure_date.S, .pdf_key.S]) | @csv' > failures.csv && \
python3 -c "
import pandas as pd
import sys
filename = sys.argv[1]
df = pd.read_csv('failures.csv')
summary = df['pdf_key'].value_counts().reset_index()
summary.columns = ['pdf_key', 'count']
summary = summary.sort_values('pdf_key')
with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    df.to_excel(writer, sheet_name='All Records', index=False)
    summary.to_excel(writer, sheet_name='Summary', index=False)
    
    # Auto-fit column widths for All Records sheet
    ws1 = writer.sheets['All Records']
    for col in ws1.columns:
        max_len = max(len(str(cell.value or '')) for cell in col)
        col_letter = col[0].column_letter
        ws1.column_dimensions[col_letter].width = max_len + 2
    
    # Auto-fit column widths for Summary sheet
    ws2 = writer.sheets['Summary']
    for col in ws2.columns:
        max_len = max(len(str(cell.value or '')) for cell in col)
        col_letter = col[0].column_letter
        ws2.column_dimensions[col_letter].width = max_len + 2
" "${FILENAME}"

mv "${FILENAME}" ~/Documents/
echo "Created ~/Documents/${FILENAME}"
