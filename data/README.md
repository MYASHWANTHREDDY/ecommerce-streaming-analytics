# Dataset

## Source

E-commerce transaction records from ExcelBI Analytics.

**Download link:** https://excelbianalytics.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/

Download the **1M-row version** (around 50–100MB, not the 624MB full file).

## Instructions

1. Download from the link above.
2. Unzip the file.
3. Rename the CSV to `dataset.csv` and place it at `data/dataset.csv` in the repo root.

## Sample

A 1,000-row sample (`data/sample_1k.csv`) is committed for quick testing without downloading the full file.

## Schema

The dataset has these columns:
- `Order ID` — unique order identifier
- `Region` — geographic region
- `Country` — country
- `Item Type` — product category
- `Sales Channel` — "Online" or "Offline"
- `Order Priority` — priority level
- `Units Sold` — quantity
- `Unit Price` — price per unit
- `Unit Cost` — cost per unit
- `Order Date` — order placed date
- `Ship Date` — shipment date

The producer will convert these rows into JSON events with fresh timestamps and intentionally corrupt ~2% of events to test data quality.
