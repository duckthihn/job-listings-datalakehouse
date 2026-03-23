Design Data Lakehouse Architecture



initial idea:
1. crawl job data from itviec and topcv
2. ingest -> bronze
3. bronze -> silver (validate + transform)
4. silver -> gold (aggragate + data modeling star schema)

outputs:
    1. Notification in Discord bot
    2. Visualize using Metabase





Languages: Python, SQL
Data Processing: Apache Spark (PySpark)
Data Quality: Great Expectations
Storage & Table Format: MinIO (S3 API), Apache Iceberg
Catalog: Hive Metastore, PostgreSQL
Query Engine: Trino
Orchestration: Apache Airflow
Infrastructure: Docker, Docker Compose
Notification/BI: Discord API, Metabase