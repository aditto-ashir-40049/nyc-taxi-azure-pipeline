# NYC Yellow Taxi - Azure Data Pipeline

## Architecture
Local CSV -> Azure Blob Storage -> Azure Data Factory -> Azure Synapse Analytics -> Power BI

## Dataset
- Source: NYC TLC Yellow Taxi Trip Data (Kaggle)
- Full size: ~1.88 GB (stored in Azure Blob Storage)
- Sample: yellow_taxi_sample.csv (50,000 rows for reference)
- Columns: 19 (VendorID, pickup/dropoff datetime, fare, tip, payment type, etc.)

## Repo Structure
- sql/ - Synapse table DDL
- scripts/ - Python preprocessing scripts
- adf/ - ADF pipeline export (added later)
- yellow_taxi_sample.csv - Sample data (50K rows)
