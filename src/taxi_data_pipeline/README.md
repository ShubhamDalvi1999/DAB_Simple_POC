# NYC Taxi Data Pipeline

This DLT (Delta Live Tables) pipeline implements a medallion architecture for NYC Taxi data.

## Architecture

```
Bronze → Silver → Gold
```

### Bronze Layer
- **bronze_taxi_trips**: Raw ingestion from `samples.nyctaxi.trips`

### Silver Layer  
- **silver_taxi_trips**: Cleaned data with calculated fields (fare_per_mile, tip_percentage)

### Gold Layer
- **gold_zone_metrics**: Aggregated business metrics by pickup zone

## Running the Pipeline

Deploy and run using Databricks Asset Bundles:

```bash
databricks bundle deploy
databricks bundle run taxi_data_pipeline
```

