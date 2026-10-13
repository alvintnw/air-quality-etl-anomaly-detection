# air-quality-etl-anomaly-detection
ETL pipeline that ingests hourly air quality data, validates it, and loads it into PostgreSQL with idempotent upserts. It detects anomalies in SQL using robust z-scores and AQI thresholds, then sends Telegram alerts and a daily digest.
