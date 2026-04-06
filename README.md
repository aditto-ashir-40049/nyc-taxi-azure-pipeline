# NYC Yellow Taxi - Azure Data Pipeline

## Architecture
Local CSV -> Azure Blob Storage -> Azure Synapse Analytics -> Power BI

## Dataset
- Source: NYC TLC Yellow Taxi Trip Data (Kaggle)
- Full size: ~1.85 GB (stored in Azure Blob Storage)
- Sample: yellow_taxi_sample.csv (50,000 rows)
- Columns: 19

## Azure Services Used
- Azure Blob Storage (nyctaxistoragead) - raw data storage
- Azure Synapse Analytics (nyctaxisynapse) - serverless SQL queries
- Power BI Desktop - dashboards and visualization

## Synapse Views Created
- yellow_taxi - raw view over CSV
- yellow_taxi_clean - filtered clean data
- daily_trips - trip counts aggregated by date
- hourly_fare - fare averages by hour of day
- payment_summary - trip breakdown by payment type
- location_summary - pickup locations with trip counts

## Power BI Dashboards
- Page 1: Trip Trends Over Time
- Page 2: Fare & Tip Analysis
- Page 3: Payment Type Breakdown
- Page 4: Location Heatmap

## Repo Structure
- sql/ - All Synapse SQL scripts
- data/ - Sample dataset (50K rows)
