"""
data.py — All data constants for the Data Engineer Desk.
Equivalent to the JS data arrays in the HTML/CSS/JS version.
"""

# ─────────────────────────────────────────────
# TOOLS & STACK
# ─────────────────────────────────────────────
TOOLS = [
    {"name": "Apache Kafka",        "cat": "streaming",      "desc": "Distributed event streaming platform. Topics are append-only logs; consumers read at their own offset. Backbone of real-time pipelines — decouples producers from consumers."},
    {"name": "Apache Spark",        "cat": "processing",     "desc": "Unified analytics engine. Runs in-memory across a cluster. Supports batch, Structured Streaming, SQL, ML and graph. PySpark is the standard DE interface."},
    {"name": "Apache Flink",        "cat": "streaming",      "desc": "Stateful stream processing at scale. Lower latency than Spark SS. Native event-time semantics, watermarks and exactly-once state. Preferred for complex stateful streaming."},
    {"name": "Apache Airflow",      "cat": "orchestration",  "desc": "Python-defined DAGs for scheduling pipelines. Industry standard since 2015. Supports sensors, hooks, operators and a monitoring UI. Managed on GCP (Composer), AWS (MWAA)."},
    {"name": "dbt",                 "cat": "transformation", "desc": "SQL-first transformation tool that runs inside your warehouse. Defines models as SELECT statements, auto-builds DAGs, runs tests, generates docs. Standard for the ELT layer."},
    {"name": "Prefect",             "cat": "orchestration",  "desc": "Modern Python-native workflow orchestration. Simpler setup than Airflow. Dynamic DAGs, automatic retries, hybrid deployment. Great for Python-heavy teams."},
    {"name": "Dagster",             "cat": "orchestration",  "desc": "Asset-oriented orchestration. You define data assets, not tasks. Auto-infers dependencies. Strong observability and lineage built-in. Fast-growing in the modern DE stack."},
    {"name": "Snowflake",           "cat": "storage",        "desc": "Cloud-native data warehouse. Separates storage and compute. Virtual warehouses scale independently. Zero-copy cloning, time travel, data sharing across accounts."},
    {"name": "BigQuery",            "cat": "storage",        "desc": "Serverless analytics warehouse on GCP. Pay per query. Excellent for large ad-hoc workloads. BI Engine for sub-second cached queries. Supports Iceberg tables natively."},
    {"name": "Redshift",            "cat": "storage",        "desc": "AWS columnar MPP warehouse. Tight integration with S3 (Spectrum for external tables). RA3 nodes separate compute and storage like Snowflake."},
    {"name": "Delta Lake",          "cat": "storage",        "desc": "Open-source ACID transaction layer on object storage by Databricks. Supports time travel, schema evolution, Z-order clustering, MERGE statements."},
    {"name": "Apache Iceberg",      "cat": "storage",        "desc": "Open table format for huge analytic datasets. Supported natively by BigQuery, Snowflake, Redshift, Trino and Spark. Hidden partitioning, schema evolution, snapshot isolation."},
    {"name": "Apache Hudi",         "cat": "storage",        "desc": "Specialises in record-level upserts on a data lake. CoW and MoR table types. Heavily used at Uber (also invented here)."},
    {"name": "Fivetran",            "cat": "ingestion",      "desc": "Fully managed ELT connectors. 500+ pre-built connectors (SaaS APIs, DBs). Handles schema drift automatically. No code needed — configure and forget."},
    {"name": "Airbyte",             "cat": "ingestion",      "desc": "Open-source data integration platform. Self-hosted or cloud. 350+ connectors. Build custom connectors with their CDK. Cost-sensitive alternative to Fivetran."},
    {"name": "Debezium",            "cat": "ingestion",      "desc": "CDC platform. Reads DB transaction logs (Postgres WAL, MySQL binlog). Streams changes to Kafka. Zero-impact on source DB."},
    {"name": "Great Expectations",  "cat": "quality",        "desc": "Data validation and documentation framework. Define expectations on datasets. Generates data docs automatically. Integrates with Airflow, dbt, and Spark."},
    {"name": "Soda",                "cat": "quality",        "desc": "Data reliability platform. YAML-based checks run inside the warehouse. Alerts on anomalies, schema changes, distribution shifts."},
    {"name": "Monte Carlo",         "cat": "quality",        "desc": "ML-powered data observability. Detects anomalies automatically without writing checks. Builds lineage graphs, identifies downstream incident impact."},
    {"name": "Trino",               "cat": "processing",     "desc": "Distributed SQL query engine (formerly PrestoSQL). Queries data in-place across S3, Postgres, Kafka, Hive, Iceberg — all with SQL. No data movement."},
    {"name": "DuckDB",              "cat": "processing",     "desc": "Fast in-process OLAP SQL engine. Reads Parquet/CSV from S3 directly. Runs locally — no cluster needed. Replacing Pandas for many DE tasks."},
    {"name": "Apache Beam",         "cat": "processing",     "desc": "Unified batch and stream processing model. Write once, run on Dataflow, Spark or Flink. Best for Google Cloud (Dataflow) deployments."},
    {"name": "Terraform",           "cat": "orchestration",  "desc": "Infrastructure as code. Define cloud resources in HCL. Apply, plan, destroy. Essential for reproducible data platform setup."},
    {"name": "DataHub",             "cat": "catalog",        "desc": "Open-source data catalog and lineage platform. Ingests metadata from 50+ sources. Tracks column-level lineage, ownership, tags, schemas."},
    {"name": "dbt Cloud",           "cat": "transformation", "desc": "Managed dbt with IDE, CI/CD, scheduler, and docs hosting. Run dbt jobs on a schedule without Airflow. Semantic Layer for shared metric definitions."},
    {"name": "OpenLineage",         "cat": "catalog",        "desc": "Open standard for data lineage metadata. Emitted by Airflow, Spark, dbt, Flink. Collected by Marquez or DataHub. Enables column-level lineage tracking."},
    {"name": "Feast",               "cat": "catalog",        "desc": "Open-source feature store. Defines, stores and serves ML features consistently. Solves training-serving skew."},
    {"name": "Apache Atlas",        "cat": "catalog",        "desc": "Data governance and metadata management. Part of Hadoop ecosystem. Common in Hadoop/Cloudera enterprise stacks."},
]

TOOL_CATEGORIES = ["All", "ingestion", "processing", "storage", "orchestration", "streaming", "transformation", "quality", "catalog"]

# ─────────────────────────────────────────────
# PIPELINE STAGES
# ─────────────────────────────────────────────
PIPELINE_STAGES = [
    {
        "label": "Source", "icon": "🗄", "sub": "Raw data origin",
        "title": "Data Sources",
        "desc": "The start of every pipeline. Sources include operational databases (OLTP), SaaS platforms, event streams, flat files, IoT sensors, and third-party APIs. Understanding source characteristics is critical: schema stability, update frequency, data volume, SLA for freshness, and whether the source supports CDC or only full dumps.",
        "tools": ["PostgreSQL", "MySQL", "MongoDB", "Oracle", "Salesforce", "Stripe API", "Kafka", "S3 files", "IoT MQTT", "Webhooks"],
        "concepts": ["Full load vs incremental", "CDC readiness", "API rate limits", "Source schema drift", "Historical backfill scope", "PII identification"],
        "pitfalls": [
            "Never query production OLTP DBs directly — always use a replica or CDC",
            "Rate limits on APIs can silently drop records — always add response validation",
            "Undocumented schema changes from 3rd-party SaaS will break your pipeline without warning",
        ],
        "code": '''# Resilient incremental API ingestion with retry + watermark
import requests, time
from datetime import datetime

def fetch_orders(watermark: datetime, page=1, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = requests.get(
                "https://api.shop.io/v2/orders",
                params={"updated_after": watermark.isoformat(),
                        "page": page, "limit": 1000, "sort": "updated_at:asc"},
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            return data["items"], data["has_next_page"]
        except requests.HTTPError as e:
            if e.response.status_code == 429:
                time.sleep(2 ** attempt)   # exponential backoff
            else:
                raise''',
        "lang": "python",
    },
    {
        "label": "Ingest", "icon": "📥", "sub": "Extract to raw landing",
        "title": "Ingestion Layer",
        "desc": "Extract data from sources and land it raw — unchanged — into a staging area (data lake or staging DB). The golden rule: land raw first, transform later (ELT). Two major strategies: Batch (scheduled full/incremental pulls) and CDC (stream every DB change via transaction log). Always make ingestion idempotent.",
        "tools": ["Fivetran", "Airbyte", "Debezium", "Kafka Connect", "AWS DMS", "Stitch", "custom Python"],
        "concepts": ["ELT vs ETL", "CDC via WAL/binlog", "Full load vs incremental", "Watermark / checkpoint", "Idempotent loads", "Audit metadata"],
        "pitfalls": [
            "Ingesting transformed data (ETL) makes reprocessing impossible — always land raw",
            "Missing a watermark update causes duplicate or skipped records",
            "No row count validation means silent data loss goes undetected",
        ],
        "code": '''# Debezium Kafka Connect config for Postgres CDC
connector_config = {
    "name": "orders-postgres-connector",
    "config": {
        "connector.class":
            "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "prod-replica.internal",
        "database.dbname":   "orders_db",
        "database.user":     "debezium_ro",
        "table.include.list": "public.orders,public.order_items",
        "publication.name":   "dbz_publication",
        "slot.name":          "debezium_orders_slot",
        "plugin.name":        "pgoutput",
        "topic.prefix":       "prod.postgres",
    }
}''',
        "lang": "python",
    },
    {
        "label": "Raw Store", "icon": "🏞", "sub": "Lake landing zone",
        "title": "Raw Storage — Landing Zone",
        "desc": "Raw data lands here unchanged on cheap object storage (S3, GCS, ADLS). This is your source of truth — the ability to reprocess everything from raw is what makes your pipeline resilient. Structure with date partitions. Never delete raw data. File format: Parquet for columnar at scale.",
        "tools": ["AWS S3", "GCS", "Azure ADLS Gen2", "MinIO (self-hosted)", "Delta Lake raw"],
        "concepts": ["Hive-style partitioning", "Parquet vs Avro vs JSON", "File size optimisation", "Data catalog registration", "Retention policy"],
        "pitfalls": [
            "Small files problem: millions of 10KB files will kill Spark performance — compact regularly",
            "No partitioning means full table scans on every query — always partition by date",
            "Forgetting to register to Glue/Hive Metastore makes discovery impossible",
        ],
        "code": '''# Write partitioned Parquet to S3 with PySpark
from pyspark.sql import functions as F
from datetime import datetime

LOAD_TS = datetime.utcnow().isoformat()

(df
  .withColumn("_etl_load_ts", F.lit(LOAD_TS))
  .withColumn("year",  F.year("event_ts"))
  .withColumn("month", F.month("event_ts"))
  .withColumn("day",   F.dayofmonth("event_ts"))
  .write.partitionBy("year", "month", "day")
  .mode("append")
  .option("compression", "snappy")
  .parquet("s3://data-lake/raw/api_orders/")
)
# Path: s3://data-lake/raw/api_orders/year=2024/month=03/day=15/''',
        "lang": "python",
    },
    {
        "label": "Transform", "icon": "⚙️", "sub": "Clean, model & enrich",
        "title": "Transformation Layer",
        "desc": "Convert raw data into clean, tested, business-ready models. The modern standard: ELT inside the warehouse using dbt. Follow the Medallion three-layer model: Bronze (raw copy), Silver (cleaned, typed, deduplicated), Gold (business aggregations, star schema). Every model must have tests.",
        "tools": ["dbt", "Apache Spark", "PySpark", "Pandas", "SQL", "dbt Cloud", "Dataform"],
        "concepts": ["Medallion: Bronze/Silver/Gold", "SCD Type 1 & 2", "Deduplication", "Star schema", "Incremental models", "Schema evolution"],
        "pitfalls": [
            "No tests on transformations means bugs silently reach dashboards",
            "Overwriting history (SCD1) when business needs change tracking (SCD2)",
            "Not using incremental models means full reprocessing every run — expensive",
        ],
        "code": '''-- dbt silver model: stg_orders.sql (incremental + dedup)
{{ config(materialized="incremental", unique_key="order_id",
          on_schema_change="sync_all_columns") }}

WITH source AS (
    SELECT * FROM {{ source("raw", "api_orders") }}
    {% if is_incremental() %}
    WHERE _etl_load_ts > (SELECT MAX(_etl_load_ts) FROM {{ this }})
    {% endif %}
),
cleaned AS (
    SELECT
        order_id,
        COALESCE(channel, "unknown")         AS channel,
        CAST(total_amount AS NUMERIC(14,2))  AS total_amount_usd,
        LOWER(TRIM(status))                  AS status,
        ordered_at::TIMESTAMP WITH TIME ZONE AS ordered_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id ORDER BY _etl_load_ts DESC
        ) AS _row_num
    FROM source WHERE order_id IS NOT NULL
)
SELECT * EXCEPT(_row_num) FROM cleaned WHERE _row_num = 1''',
        "lang": "sql",
    },
    {
        "label": "Orchestrate", "icon": "🕹", "sub": "Schedule, retry, monitor",
        "title": "Orchestration Layer",
        "desc": "Orchestrators schedule tasks, enforce dependencies, handle retries, send alerts and provide visibility into your pipeline's health. The core primitive is a DAG. Every task must be idempotent — safe to re-run. Design for failure: set timeouts, retry counts, and SLA breach alerts.",
        "tools": ["Apache Airflow", "Prefect", "Dagster", "Argo Workflows", "AWS Step Functions", "Temporal"],
        "concepts": ["DAG design", "Task dependencies", "Idempotent tasks", "Backfill / catchup", "SLA monitoring", "Alerting on failure"],
        "pitfalls": [
            "catchup=True on Airflow DAGs will trigger hundreds of backfill runs on deploy — disable it",
            "Non-idempotent tasks cause duplicate data when retried",
            "No SLA alerts means you find out pipeline failures from an angry stakeholder",
        ],
        "code": '''# Production Airflow DAG — orders nightly pipeline
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "data-engineering", "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-oncall@company.com"],
    "execution_timeout": timedelta(hours=2),
}

with DAG(dag_id="orders_nightly_pipeline",
         schedule_interval="0 2 * * *",
         start_date=days_ago(1),
         catchup=False,                  # NEVER run missed intervals on deploy
         default_args=DEFAULT_ARGS) as dag:

    extract   = PythonOperator(task_id="extract_orders",   python_callable=run_extraction)
    transform = PythonOperator(task_id="dbt_transform",    python_callable=run_dbt_models)
    quality   = PythonOperator(task_id="quality_checks",   python_callable=run_soda_checks)

    extract >> transform >> quality''',
        "lang": "python",
    },
    {
        "label": "Warehouse", "icon": "🏛", "sub": "Curated analytics store",
        "title": "Data Warehouse / Lakehouse",
        "desc": "The serving layer for analytics. Data here is clean, tested, and modeled for BI tools and data scientists. Key concerns: query performance (clustering, partitioning, materialized views), cost governance (query scanning limits), and access control (row-level security, column masking).",
        "tools": ["Snowflake", "BigQuery", "Redshift", "Databricks", "ClickHouse", "Synapse"],
        "concepts": ["OLAP vs OLTP", "Columnar storage", "Clustering keys", "Partitioning", "Materialized views", "Row-level security", "Column masking"],
        "pitfalls": [
            "No clustering/partitioning on large tables makes every query scan the full dataset",
            "Allowing unrestricted ad-hoc queries causes runaway cloud costs",
            "No column masking on PII tables is a compliance violation",
        ],
        "code": '''-- Snowflake: clustered table + row-level security + materialized view
CREATE OR REPLACE TABLE analytics.fct_orders (
    order_id     VARCHAR(36) NOT NULL,
    customer_id  VARCHAR(36),
    total_amount NUMBER(14,2),
    status       VARCHAR(30),
    ordered_at   TIMESTAMP_TZ,
    region       VARCHAR(30)
) CLUSTER BY (TO_DATE(ordered_at), region);

-- Row access policy (users see only their region)
CREATE OR REPLACE ROW ACCESS POLICY rls.region_filter
    AS (region VARCHAR) RETURNS BOOLEAN ->
    region = CURRENT_ROLE() OR CURRENT_ROLE() = "DATA_ADMIN";

-- Materialized view for weekly KPI
CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_weekly_revenue AS
SELECT DATE_TRUNC("week", ordered_at) AS week_start, region,
       COUNT(DISTINCT order_id) AS orders,
       SUM(total_amount) AS revenue_usd
FROM analytics.fct_orders WHERE status = "completed"
GROUP BY 1, 2;''',
        "lang": "sql",
    },
    {
        "label": "Serve", "icon": "📊", "sub": "BI, ML & APIs",
        "title": "Serving & Consumption Layer",
        "desc": "Downstream consumers of your curated data. BI tools query the warehouse via SQL. ML pipelines read feature tables. Operational APIs serve low-latency lookups from Redis or a feature store. A semantic layer (dbt Semantic Layer, Cube.js, LookML) centralises metric definitions so 'revenue' means the same everywhere.",
        "tools": ["Tableau", "Looker", "Metabase", "Power BI", "Grafana", "Feast", "Redis", "FastAPI"],
        "concepts": ["Semantic layer", "Feature stores", "Data products", "Data contracts", "SLA / freshness SLOs", "Reverse ETL"],
        "pitfalls": [
            "No semantic layer leads to 5 different definitions of 'revenue' in 5 dashboards",
            "Feature skew: training features differ from serving features — causes model degradation",
            "No data contracts means consumers break silently when upstream schema changes",
        ],
        "code": '''# FastAPI serving warehouse data with Redis caching
from fastapi import FastAPI
import snowflake.connector, redis, json, hashlib

app   = FastAPI()
cache = redis.Redis(host="redis.internal", port=6379, decode_responses=True)

@app.get("/v1/metrics/weekly-revenue")
async def weekly_revenue(region: str, weeks: int = 12):
    key = hashlib.md5(f"rev:{region}:{weeks}".encode()).hexdigest()
    if cached := cache.get(key):
        return json.loads(cached)

    cur = get_db().cursor()
    cur.execute("""
        SELECT week_start, orders, revenue_usd
        FROM   analytics.mv_weekly_revenue
        WHERE  region = %s AND week_start >= DATEADD("week", -%s, CURRENT_DATE)
        ORDER  BY week_start DESC
    """, (region, weeks))
    rows = [{"week": str(r[0]), "orders": r[1], "revenue": float(r[2])} for r in cur]
    cache.setex(key, 300, json.dumps(rows))   # 5-min TTL
    return rows''',
        "lang": "python",
    },
]

# ─────────────────────────────────────────────
# ARCHITECTURE
# ─────────────────────────────────────────────
ARCH_DATA = {
    "Lambda": {
        "desc": "Batch + Speed layers merged at Serving. Two separate codebases handle historical accuracy and real-time updates.",
        "dot": """digraph {
    rankdir=LR; bgcolor=transparent;
    node [shape=box style=filled fillcolor="#1e3150" fontcolor="#c8ddf0" color="#00d4b4" fontname="monospace" fontsize=11]
    edge [color="#8899aa" fontsize=10 fontcolor="#8899aa"]
    Source [label="Source"] Kafka [label="Kafka\\nevent bus"]
    Batch [label="Batch Layer\\nSpark / Hive"] Speed [label="Speed Layer\\nFlink / Spark SS"]
    Serving [label="Serving Layer\\nWarehouse / API"] App [label="App / BI"]
    Source -> Kafka
    Kafka -> Batch [label="batch"]
    Kafka -> Speed [label="stream"]
    Batch -> Serving Speed -> Serving Serving -> App
}""",
        "when": "Need both historical reprocessing accuracy AND sub-second real-time views. Classic for analytics + operational dashboards running in parallel.",
        "tradeoffs": ["Two codebases to maintain (batch + stream)", "Data consistency risk between layers", "High operational complexity", "Expensive compute duplication"],
        "usage": "LinkedIn (early news feed), Netflix (viewing history), Twitter (timeline ranking). Most have since migrated to Kappa or Lakehouse.",
        "tools": "Kafka · Spark · Flink · Hadoop · HBase · Cassandra · Druid",
    },
    "Kappa": {
        "desc": "Everything is a stream. Historical reprocessing = replay Kafka from any offset. One codebase.",
        "dot": """digraph {
    rankdir=LR; bgcolor=transparent;
    node [shape=box style=filled fillcolor="#1e3150" fontcolor="#c8ddf0" color="#00d4b4" fontname="monospace" fontsize=11]
    edge [color="#8899aa" fontsize=10 fontcolor="#8899aa"]
    Source Kafka [label="Kafka\\nimmutable log"] Processor [label="Stream Processor\\nFlink / Spark SS"]
    Serving [label="Serving\\nDruid / Redis"] App [label="App"]
    Source -> Kafka -> Processor -> Serving -> App
    Kafka -> Processor [label="replay from offset" style=dashed color="#556677"]
}""",
        "when": "Single codebase. All processing is streaming. Historical reprocessing = replay Kafka from an offset. Simpler to operate than Lambda.",
        "tradeoffs": ["Kafka retention cost for large history", "Complex stateful joins are harder", "Stream processing expertise needed", "Not ideal for heavy batch aggregations"],
        "usage": "Uber (surge pricing), Twitter (real-time ads), Cloudflare (analytics pipeline). Popularised by Jay Kreps (Kafka creator) in 2014.",
        "tools": "Kafka · Apache Flink · Spark Structured Streaming · Apache Druid · ksqlDB",
    },
    "Lakehouse": {
        "desc": "Open object storage + ACID transactions via Delta/Iceberg. One copy of data serves BI, ML and data science.",
        "dot": """digraph {
    bgcolor=transparent;
    node [shape=box style=filled fillcolor="#1e3150" fontcolor="#c8ddf0" color="#00d4b4" fontname="monospace" fontsize=11]
    edge [color="#8899aa" fontsize=10 fontcolor="#8899aa"]
    Sources [label="Sources\\nDB / APIs / Streams"]
    Storage [label="Object Storage\\nS3 / GCS / ADLS\\n+ Delta / Iceberg"]
    Meta [label="Metadata\\nUnity Catalog / Glue"]
    Spark [label="Spark\\nbatch/stream"] Trino [label="Trino / dbt\\nSQL query"]
    Consumers [label="BI · ML · Dashboards"]
    Sources -> Storage Meta -> Storage
    Spark -> Storage [dir=both] Trino -> Storage [dir=both]
    Storage -> Consumers
}""",
        "when": "Modern default. One copy of data on cheap object storage with ACID transactions. Serves BI, SQL analysts, ML engineers and data scientists from one platform.",
        "tradeoffs": ["Small-file problem on high-frequency writes", "Metadata management complexity", "VACUUM / OPTIMIZE jobs needed", "Compute cost for heavy transforms"],
        "usage": "Airbnb (Delta Lake), Stripe (Iceberg on S3), Coinbase (Databricks Lakehouse), DoorDash (dbt + BigQuery). Industry standard as of 2023–24.",
        "tools": "Delta Lake · Apache Iceberg · Apache Hudi · Databricks · Trino · dbt · Spark",
    },
    "Data Mesh": {
        "desc": "Domain teams own and serve their own data as products. A central self-serve platform provides the infrastructure.",
        "dot": """digraph {
    bgcolor=transparent;
    node [shape=box style=filled fillcolor="#1e3150" fontcolor="#c8ddf0" color="#00d4b4" fontname="monospace" fontsize=11]
    edge [color="#8899aa" fontsize=10 fontcolor="#8899aa"]
    Platform [label="Self-serve Data Platform\\nInfra / Catalog / Governance" style="filled,dashed"]
    Orders [label="Orders\\ndomain team"] Marketing [label="Marketing\\ndomain team"]
    Payments [label="Payments\\ndomain team"] Analytics [label="Analytics\\ndomain team"]
    Orders -> Platform [style=dashed] Marketing -> Platform [style=dashed]
    Payments -> Platform [style=dashed] Analytics -> Platform [style=dashed]
}""",
        "when": "Large orgs where a central data team is a bottleneck. Domains take ownership of pipelines and data quality. Requires strong platform team underneath.",
        "tradeoffs": ["Organisational change required", "Risk of inconsistent data definitions", "High platform investment upfront", "Federated governance is hard to enforce"],
        "usage": "Zalando (public case study), JPMorgan Chase, Saxo Bank. Coined by Zhamak Dehghani in 2019. Still emerging — not universally adopted.",
        "tools": "DataHub · Atlan · dbt · Backstage · Dataplex (GCP) · Microsoft Purview",
    },
}

# ─────────────────────────────────────────────
# CLOUD & SERVICES
# ─────────────────────────────────────────────
CLOUD_DATA = [
    {
        "name": "AWS", "services": [
            {"name": "S3",              "cat": "storage",       "use": "Object storage for raw data lake, staging, archive.",             "when": "Default raw storage. Pair with Glue Catalog. Lifecycle rules move old partitions to Glacier."},
            {"name": "Glue",            "cat": "etl",           "use": "Managed Spark ETL + Data Catalog (Hive Metastore compatible).",   "when": "Use Glue Catalog as central metastore. Glue ETL for lighter jobs; prefer EMR for heavy Spark workloads."},
            {"name": "Kinesis Streams", "cat": "streaming",     "use": "Real-time data streams — managed Kafka alternative.",             "when": "Use when staying fully in AWS without Kafka expertise. Lower throughput ceiling than MSK."},
            {"name": "MSK",             "cat": "streaming",     "use": "Managed Kafka — full Kafka API for high-throughput streaming.",   "when": "Prefer MSK when team knows Kafka, needs Kafka Connect, or migrating from on-prem Kafka."},
            {"name": "EMR",             "cat": "processing",    "use": "Managed Spark/Hadoop cluster. Supports Delta Lake, Iceberg.",     "when": "Heavy Spark batch/streaming jobs. Cost-effective with Spot instances. Pair with S3 as lake."},
            {"name": "Redshift",        "cat": "warehouse",     "use": "Columnar MPP warehouse. Spectrum for external S3 tables.",        "when": "Best when workload is SQL-heavy and team is already on AWS. Less elastic than Snowflake/BigQuery."},
            {"name": "Athena",          "cat": "query",         "use": "Serverless SQL on S3 via Presto. Pay per TB scanned.",           "when": "Ad-hoc queries on raw lake without loading into warehouse. Use columnar Parquet to minimise cost."},
            {"name": "DMS",             "cat": "ingestion",     "use": "Database Migration Service — ongoing CDC to targets.",            "when": "Simpler CDC than Debezium for AWS-native setups. Use Debezium+MSK when stream fan-out needed."},
            {"name": "Lambda",          "cat": "compute",       "use": "Serverless functions — event triggers, light transforms.",       "when": "Small event-driven tasks: trigger a pipeline on S3 upload, validate and route events."},
        ],
    },
    {
        "name": "GCP", "services": [
            {"name": "BigQuery",  "cat": "warehouse",     "use": "Serverless MPP warehouse. Pay per query. Supports Iceberg.",         "when": "Best analytics warehouse for orgs on GCP. Sub-second queries with BI Engine cache. Native BQML."},
            {"name": "Dataflow",  "cat": "processing",    "use": "Managed Apache Beam. Unified batch + streaming. Auto-scales.",       "when": "When using Beam SDK for portable batch/stream pipelines. Tightly integrated with Pub/Sub + BigQuery."},
            {"name": "Pub/Sub",   "cat": "streaming",     "use": "Managed message queue for event ingestion into GCP pipelines.",      "when": "Ingest streaming events into GCP. Pair with Dataflow for real-time ETL into BigQuery."},
            {"name": "GCS",       "cat": "storage",       "use": "Google Cloud Storage. Raw lake and staging for GCP pipelines.",     "when": "Default object storage on GCP. Same Parquet + Iceberg patterns as S3. Nearline/Coldline for archival."},
            {"name": "Dataproc",  "cat": "processing",    "use": "Managed Spark/Hadoop on GCP. Ephemeral clusters.",                  "when": "Heavy Spark batch jobs on GCP. Serverless removes cluster management — pay per vCPU-second."},
            {"name": "Composer",  "cat": "orchestration", "use": "Managed Apache Airflow on GCP.",                                   "when": "Standard Airflow on GCP without ops burden. Slightly expensive — evaluate self-hosted on GKE."},
            {"name": "Bigtable",  "cat": "storage",       "use": "NoSQL wide-column for low-latency time-series and lookups.",        "when": "Serving layer for ML features at low latency. IoT time-series. Not a BigQuery replacement."},
            {"name": "Looker",    "cat": "bi",            "use": "BI platform with LookML semantic layer. Metric definitions as code.", "when": "Best when you need a centralised semantic layer. LookML enforces consistent metric definitions."},
        ],
    },
    {
        "name": "Azure", "services": [
            {"name": "ADLS Gen2",        "cat": "storage",       "use": "Azure Data Lake Storage Gen2. Hierarchical namespace.",          "when": "Default raw lake on Azure. Pair with Databricks or Synapse. Supports Delta Lake and Iceberg natively."},
            {"name": "Synapse Analytics","cat": "warehouse",     "use": "Unified analytics — dedicated SQL pool + Spark + serverless SQL.","when": "All-in-one platform for Azure-native teams. Dedicated SQL for warehouse, Spark for ETL."},
            {"name": "Event Hubs",       "cat": "streaming",     "use": "Kafka-compatible managed event streaming.",                       "when": "Use when migrating from on-prem Kafka or needing Kafka protocol on Azure."},
            {"name": "Data Factory",     "cat": "ingestion",     "use": "Cloud ETL/ELT orchestration — 90+ connectors, visual designer.", "when": "Low-code data movement and orchestration on Azure. Good for non-Python teams."},
            {"name": "Databricks",       "cat": "processing",    "use": "Best-in-class Spark + Delta Lake. Unity Catalog for governance.","when": "Premium Spark platform. Use when workload demands Delta Lake, Unity Catalog, or ML + DE together."},
            {"name": "Microsoft Fabric", "cat": "warehouse",     "use": "New unified SaaS analytics — Lakehouse + Warehouse + Power BI.","when": "Emerging option for Azure-native orgs on OneLake. Evaluate for new greenfield builds."},
            {"name": "Stream Analytics", "cat": "streaming",     "use": "Real-time SQL queries on event streams from Event Hubs.",        "when": "Simple real-time aggregations without Flink/Spark expertise. Low latency, fully managed."},
            {"name": "Purview",          "cat": "catalog",       "use": "Data catalog, lineage and governance across Azure services.",    "when": "Enterprise governance on Azure. Scans ADLS, Synapse, SQL Server. Column-level lineage."},
        ],
    },
]

# ─────────────────────────────────────────────
# DESIGN PATTERNS
# ─────────────────────────────────────────────
DESIGN_PATTERNS = [
    {
        "name": "Medallion Architecture", "sub": "Bronze → Silver → Gold",
        "desc": "A three-layer data lake pattern that progressively refines data quality and structure.",
        "detail": "Bronze: raw data landed as-is. Silver: cleaned, typed, deduplicated. Gold: aggregated business KPIs, star schema models. Each layer is independently queryable and reprocessable from the layer below.",
        "when": "Default pattern for Lakehouse architectures. If Gold breaks, reprocess from Silver.",
        "tools": ["Delta Lake / Databricks", "dbt on Snowflake or BigQuery", "Any Lakehouse setup"],
        "pitfall": "Don't skip Silver and write business logic directly into Gold — you lose the clean foundation.",
    },
    {
        "name": "Slowly Changing Dimension", "sub": "SCD Type 1, 2 & 3",
        "desc": "How to handle history when dimension data changes over time.",
        "detail": "SCD1: Overwrite — no history. SCD2: New row with effective_from, expiry_date, is_current — full history. SCD3: Previous column — one change only. SCD2 is the industry standard for customer, product and geography dimensions.",
        "when": "Any dimension table where business needs 'what was true at time of order'.",
        "tools": ["dbt snapshots (built-in SCD2)", "Apache Hudi MoR", "Manually in Spark MERGE"],
        "pitfall": "Using SCD1 when users ask 'why did this customer's segment change?' — impossible without SCD2.",
    },
    {
        "name": "Change Data Capture", "sub": "Stream every DB mutation",
        "desc": "Capture every INSERT, UPDATE, DELETE from a source database in real-time via its transaction log.",
        "detail": "Instead of polling with SELECT WHERE updated_at > watermark (misses deletes), CDC reads the Write-Ahead Log. Every row mutation becomes a Kafka event. Downstream consumers process inserts, updates and deletes independently.",
        "when": "When source DB can't expose a reliable updated_at, when you need to capture hard deletes, or latency < 30s is required.",
        "tools": ["Debezium + Kafka", "AWS DMS", "Striim", "Qlik Replicate"],
        "pitfall": "CDC requires enabling logical replication on the source DB — coordinate with the DB team before enabling in production.",
    },
    {
        "name": "Incremental Load Pattern", "sub": "Process only new/changed rows",
        "desc": "Only extract and process records that changed since the last pipeline run, using a watermark.",
        "detail": "Store last_processed_at in a checkpoint table. On each run: SELECT WHERE updated_at > watermark. After successful load, update checkpoint. In dbt: use is_incremental() macro. In Kafka: consumer offset IS the watermark.",
        "when": "Any table too large to full-refresh every run. Standard for all production pipelines.",
        "tools": ["dbt incremental models", "Spark with partition pruning", "Kafka consumer offsets"],
        "pitfall": "If source lacks a reliable updated_at column, incremental logic silently misses changes.",
    },
    {
        "name": "Reverse ETL", "sub": "Warehouse data → operational tools",
        "desc": "Push curated warehouse data back into operational SaaS systems (CRM, marketing, support tools).",
        "detail": "Analytics computes customer segments, lead scores, churn predictions in the warehouse. Reverse ETL syncs these back to Salesforce, HubSpot, Intercom so teams can act on them.",
        "when": "When business teams need warehouse-computed insights in their operational tools — not just dashboards.",
        "tools": ["Census", "Hightouch", "Segment", "Custom Python + API"],
        "pitfall": "No deduplication logic in Reverse ETL causes thousands of duplicate records synced to CRM.",
    },
    {
        "name": "Data Contract Pattern", "sub": "Schema agreement as code",
        "desc": "A formal, version-controlled agreement between data producers and consumers specifying schema, SLAs and quality rules.",
        "detail": "Producer team commits to: specific column names/types, freshness SLA, quality guarantees and a deprecation policy. Tools enforce contracts in CI/CD — merge fails if contract is violated.",
        "when": "Multiple teams consuming the same dataset. Any Tier-1 data product that others depend on.",
        "tools": ["Soda Contracts", "Great Expectations", "dbt tests as contracts", "custom YAML spec"],
        "pitfall": "Contracts without enforcement are just documentation. Hook validation into CI/CD.",
    },
    {
        "name": "Partitioning Strategy", "sub": "Organise data for fast queries",
        "desc": "Divide large datasets into logical segments so queries only scan relevant data.",
        "detail": "Hive-style on object storage: /year=2024/month=03/day=15/ — queries skip all other partitions. In warehouses: PARTITION BY DATE(ordered_at). Too-granular = small files problem.",
        "when": "Any table > 1GB. Always partition by the most common filter column — usually a date column.",
        "tools": ["S3 Hive partitions", "BigQuery PARTITION BY", "Snowflake CLUSTER BY", "Iceberg hidden partitioning"],
        "pitfall": "Partitioning by high-cardinality column like customer_id creates millions of tiny partitions.",
    },
    {
        "name": "Event Sourcing", "sub": "State as a log of events",
        "desc": "Instead of storing current state, store an immutable log of every event. Derive state by replaying events.",
        "detail": "Every state change is an event stored in an append-only log (Kafka). Current state is computed by folding events. Enables time travel — replay from any point to see historical state.",
        "when": "Event-driven microservices. Audit trail requirements. When you need to rebuild any state from history.",
        "tools": ["Apache Kafka", "AWS Kinesis", "EventStore"],
        "pitfall": "Event schema changes break replay unless you version events and maintain backward compatibility.",
    },
]

# ─────────────────────────────────────────────
# ENGINEERING PRINCIPLES
# ─────────────────────────────────────────────
PRINCIPLES = [
    {
        "name": "Idempotency", "tagline": "safe to re-run, always",
        "desc": "An idempotent pipeline produces the same result whether run once or ten times. Non-negotiable for any production pipeline — retries, backfills and incident recovery all depend on it.",
        "dos": ["Use MERGE/UPSERT instead of INSERT", "Write to unique partition paths on the lake", "Store and check watermarks before writing", "Delete-then-insert on target partitions"],
        "donts": ["Append without dedup logic — causes duplicate rows", "Rely on 'it won't run twice' — it will, eventually"],
        "example": '''-- Idempotent MERGE in Snowflake
MERGE INTO analytics.fct_orders AS target
USING staging.new_orders AS src
  ON  target.order_id = src.order_id
WHEN MATCHED    THEN UPDATE SET status = src.status, ...
WHEN NOT MATCHED THEN INSERT (order_id, ...) VALUES (src.order_id, ...);''',
        "example_lang": "sql",
    },
    {
        "name": "Observability", "tagline": "you can't fix what you can't see",
        "desc": "Every pipeline must emit metadata that answers: did it run? did it complete on time? how many rows were processed? were any quality checks violated?",
        "dos": ["Log row counts at each stage", "Alert on SLA breach (freshness SLO)", "Track schema changes automatically", "Emit OpenLineage events for lineage tracking"],
        "donts": ["Silent success — log nothing after a run", "Alerting only on exceptions (misses data quality issues)"],
        "example": '''# Log pipeline metadata to an audit table
def log_pipeline_run(pipeline, rows_in, rows_out, status, error=None):
    conn.execute("""
        INSERT INTO ops.pipeline_audit
          (pipeline_name, run_ts, rows_in, rows_out, status, error_msg)
        VALUES (%s, NOW(), %s, %s, %s, %s)
    """, [pipeline, rows_in, rows_out, status, str(error)])''',
        "example_lang": "python",
    },
    {
        "name": "Schema Evolution", "tagline": "don't break when upstream changes",
        "desc": "Sources change their schemas constantly — new columns appear, types change, columns get removed. Your pipeline must handle this without breaking downstream consumers.",
        "dos": ["Use schema-on-read formats (Parquet, Delta, Iceberg)", "Track schema with Schema Registry (Avro + Confluent)", "Use dbt on_schema_change='sync_all_columns'", "Validate schema on ingest and alert on unexpected changes"],
        "donts": ["Hard-code column positions (SELECT col1, col2 from CSV)", "DROP TABLE and recreate on every run"],
        "example": '''# Detect schema drift on ingestion
def validate_schema(df, expected_schema_path):
    import json
    with open(expected_schema_path) as f:
        expected = json.load(f)
    incoming  = {f.name: str(f.dataType) for f in df.schema}
    missing   = set(expected) - set(incoming)
    type_diff = {c for c in incoming if c in expected and incoming[c] != expected[c]}
    if missing or type_diff:
        raise ValueError(f"Schema violation — Missing: {missing}, TypeChanged: {type_diff}")''',
        "example_lang": "python",
    },
    {
        "name": "Data Quality", "tagline": "garbage in, garbage out",
        "desc": "Automated checks must run after every pipeline run and block downstream promotion on failure. Quality includes freshness, completeness, uniqueness, referential integrity, and statistical distribution.",
        "dos": ["Define quality rules before writing the pipeline", "Test: not_null, unique, referential integrity, range", "Block Gold layer promotion if Silver quality fails", "Track quality metrics over time to catch gradual drift"],
        "donts": ["Manual ad-hoc quality checks — they rot", "Running checks but not acting on failures"],
        "example": '''# soda check file — runs after every dbt build
checks for fct_orders:
  - row_count > 0
  - missing_count(order_id) = 0
  - duplicate_count(order_id) = 0
  - min(total_amount_usd) >= 0
  - freshness(ordered_at) < 4h
  - anomaly detection for row_count''',
        "example_lang": "yaml",
    },
    {
        "name": "Backfill Strategy", "tagline": "reprocess history safely",
        "desc": "Every production pipeline will need to backfill — after a bug fix, a new metric, or a data source change. Design for backfill from day one.",
        "dos": ["Design incremental logic to accept a date override param", "Write to partitioned paths so backfill overwrites cleanly", "Backfill in small date chunks, not one massive run", "Test backfill in staging before running on production"],
        "donts": ["Write to non-partitioned targets — impossible to safely backfill", "Assume backfills won't happen — they always do"],
        "example": '''# Airflow backfill CLI — reprocess last 30 days
airflow dags backfill \\
  --dag-id orders_nightly_pipeline \\
  --start-date 2024-02-01 \\
  --end-date   2024-03-01 \\
  --reset-dagruns''',
        "example_lang": "bash",
    },
    {
        "name": "Data Security & Compliance", "tagline": "protect data at every layer",
        "desc": "PII and sensitive data must be identified, classified, masked and access-controlled throughout the pipeline — not just at the serving layer.",
        "dos": ["Classify PII columns in your data catalog (tag: pii=true)", "Apply column masking policies in the warehouse", "Encrypt at rest and in transit (TLS + S3 SSE-KMS)", "Implement right-to-erasure: DELETE + VACUUM on Delta/Iceberg"],
        "donts": ["Store raw PII in unencrypted flat files", "Allow all engineers full SELECT on production tables"],
        "example": '''-- Snowflake dynamic data masking for PII
CREATE MASKING POLICY email_mask AS (val STRING)
RETURNS STRING ->
  CASE WHEN CURRENT_ROLE() IN ("DATA_ADMIN") THEN val
       ELSE REGEXP_REPLACE(val, ".+@", "****@")
  END;

ALTER TABLE analytics.dim_customers
MODIFY COLUMN email SET MASKING POLICY email_mask;''',
        "example_lang": "sql",
    },
]

# ─────────────────────────────────────────────
# GLOSSARY
# ─────────────────────────────────────────────
GLOSSARY = [
    {"term": "ETL", "short": "Extract · Transform · Load", "refs": ["ELT", "Spark", "dbt"],
     "def": "The classic pipeline pattern: extract from sources, transform in a processing engine (Spark, Python), then load the result into the destination. Contrast with ELT where you load raw first then transform inside the warehouse.",
     "example": "# ETL: transform BEFORE loading\ntransformed_df = raw_df.filter(...).groupBy(...).agg(...)\ntransformed_df.write.jdbc(url=warehouse_url, table='orders_agg')"},
    {"term": "ELT", "short": "Extract · Load · Transform", "refs": ["dbt", "Snowflake", "BigQuery"],
     "def": "Modern approach: extract raw data, load it as-is into a warehouse or lake (fast, cheap), then transform using SQL inside the warehouse (dbt). Preferred because raw data is preserved and transformations can be iterated without re-ingesting.",
     "example": "-- ELT: load raw, transform inside warehouse via dbt\nCOPY INTO raw.orders FROM @stage/orders.parquet;\n-- dbt model reads raw and outputs clean Silver table"},
    {"term": "CDC", "short": "Change Data Capture", "refs": ["Debezium", "Kafka", "Watermark"],
     "def": "Technique to capture every INSERT/UPDATE/DELETE from a source database by reading its write-ahead log (WAL in Postgres, binlog in MySQL). More reliable than polling: captures hard deletes, imposes zero load on the source DB, provides < 1s latency.",
     "example": '# Kafka topic message from Debezium CDC\n{"op": "u",  # c=create, u=update, d=delete\n "before": {"status": "pending"},\n "after":  {"status": "shipped"}}'},
    {"term": "Data Lakehouse", "short": "lake + warehouse features", "refs": ["Delta Lake", "Apache Iceberg", "Medallion Architecture"],
     "def": "Architecture merging a Data Lake (cheap, flexible object storage) with Data Warehouse features (ACID transactions, schema enforcement, SQL, versioning). Achieved via open table formats: Delta Lake, Apache Iceberg, Apache Hudi on S3/GCS.",
     "example": "# Register Delta table on S3 as a SQL table\nspark.sql(\"\"\"\n  CREATE TABLE IF NOT EXISTS silver.orders\n  USING DELTA LOCATION 's3://lake/silver/orders/'\n\"\"\")"},
    {"term": "DAG", "short": "Directed Acyclic Graph", "refs": ["Airflow", "Prefect", "Dagster"],
     "def": "Core primitive in orchestration. Tasks = nodes. Dependencies = directed edges. Acyclic = no circular dependencies. The DAG defines what runs, in what order, and what to do on failure.",
     "example": "# Airflow DAG dependency chain:\nextract >> validate >> transform >> quality_check >> notify"},
    {"term": "Idempotency", "short": "safe to re-run always", "refs": ["Backfill Strategy", "Orchestration"],
     "def": "A pipeline is idempotent if running it N times produces the same result as running it once. Essential for retry logic, backfills, incident recovery. Achieved via MERGE/UPSERT, writing to partitioned paths, and storing watermarks atomically.",
     "example": "-- Non-idempotent (BAD): double-run = duplicate rows\nINSERT INTO fct_orders SELECT * FROM staging.new_orders;\n\n-- Idempotent (GOOD): re-run is safe\nMERGE INTO fct_orders t USING staging.new_orders s ON t.order_id = s.order_id\nWHEN MATCHED THEN UPDATE SET ...\nWHEN NOT MATCHED THEN INSERT ...;"},
    {"term": "SCD", "short": "Slowly Changing Dimension", "refs": ["dbt snapshots", "Transformation"],
     "def": "How to handle history when dimension attributes change. Type 1: overwrite — no history. Type 2: add new row with effective_from, effective_to, is_current — full history preserved. Type 3: add prev_value column — one change only.",
     "example": "-- SCD2 dimension structure\ncustomer_id | segment | effective_from | effective_to | is_current\nC42         | silver  | 2023-01-01     | 2023-06-14   | false\nC42         | gold    | 2023-06-15     | 9999-12-31   | true"},
    {"term": "Watermark", "short": "incremental load boundary", "refs": ["Incremental Load Pattern", "CDC", "Backfill Strategy"],
     "def": "A stored timestamp (or ID) representing the last successfully processed record. On next run: SELECT WHERE updated_at > watermark. After successful load: UPDATE watermark to MAX(updated_at). In Kafka: the consumer offset is the watermark automatically.",
     "example": "-- Checkpoint table pattern\nSELECT max_updated_at FROM ops.pipeline_checkpoints WHERE pipeline='orders';\n-- Load WHERE updated_at > that value, then:\nUPDATE ops.pipeline_checkpoints SET max_updated_at = NOW() WHERE pipeline = 'orders';"},
    {"term": "Schema Registry", "short": "versioned schema store", "refs": ["Schema Evolution", "Kafka", "CDC"],
     "def": "A central store for Avro/Protobuf/JSON Schema definitions used in Kafka topics. Ensures producers and consumers agree on schema. Prevents deserialisation failures. Confluent Schema Registry is the standard.",
     "example": "# Register Avro schema with Schema Registry\ncurl -X POST http://schema-registry:8081/subjects/orders-value/versions \\\n  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \\\n  -d '{\"schema\": \"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"Order\\\",\\\"fields\\\":[...]}'"},
    {"term": "Partitioning", "short": "organise data for fast reads", "refs": ["Raw Store", "Medallion Architecture"],
     "def": "Dividing data into logical segments so queries scan only relevant data. On object storage (Hive-style): /year=2024/month=03/day=15/ — queries with a date filter skip all other partitions entirely.",
     "example": "# Hive-style partition path on S3\ns3://data-lake/silver/orders/year=2024/month=03/day=15/part-00000.parquet\n\n-- BigQuery partition + cluster\nCREATE TABLE orders PARTITION BY DATE(ordered_at)\nCLUSTER BY region, status;"},
    {"term": "Data Lineage", "short": "where did this data come from?", "refs": ["Observability", "DataHub"],
     "def": "Tracking the origin, movement and transformation of every field across all pipeline stages. Column-level lineage answers: 'where does revenue in this dashboard come from?' Used for impact analysis, compliance and debugging.",
     "example": '# OpenLineage event emitted by Airflow\n{"eventType": "COMPLETE",\n "job":     {"name": "orders_nightly.transform"},\n "inputs":  [{"name": "raw.api_orders"}],\n "outputs": [{"name": "silver.stg_orders"}]}'},
    {"term": "Star Schema", "short": "facts surrounded by dimensions", "refs": ["Transformation", "SCD"],
     "def": "The standard data modeling pattern for analytics warehouses. Central fact table (orders, events, page views) contains metrics and foreign keys. Surrounded by dimension tables (customers, products, dates, geography).",
     "example": "-- Star schema query\nSELECT p.category, t.month, SUM(f.revenue)\nFROM  fct_orders f\nJOIN  dim_products p ON f.product_id = p.product_id\nJOIN  dim_dates    t ON f.ordered_at = t.date\nWHERE t.year = 2024 GROUP BY 1, 2;"},
    {"term": "Data Contract", "short": "producer-consumer agreement", "refs": ["Data Quality", "Observability"],
     "def": "A formal, version-controlled specification between a data producer team and consumer teams. Specifies: schema, SLAs, quality rules and a deprecation policy. Enforced in CI/CD — a PR that violates the contract fails automatically.",
     "example": "# data-contract.yaml\ndataset: orders.fct_orders\nowner: data-engineering@company.com\nsla:\n  freshness: 'available by 06:00 UTC daily'\ncolumns:\n  order_id:     {type: string, nullable: false, unique: true}\n  total_amount: {type: decimal, nullable: false, min: 0}"},
    {"term": "Backfill", "short": "reprocess historical data safely", "refs": ["Idempotency", "Orchestration"],
     "def": "Running a pipeline over historical data after a bug fix, new metric definition, or schema migration. Requires: idempotent writes, orchestrator support for date ranges, and partition-aware processing.",
     "example": "# Airflow CLI backfill\nairflow dags backfill \\\n  --dag-id orders_nightly_pipeline \\\n  --start-date 2024-01-01 \\\n  --end-date   2024-03-31 \\\n  --reset-dagruns"},
    {"term": "Feature Store", "short": "ML feature serving layer", "refs": ["Serving Layer", "Data Contract"],
     "def": "A system for defining, computing, storing and serving ML features consistently between training and serving. Solves the critical training-serving skew problem. Tools: Feast (open source), Tecton, Databricks Feature Store.",
     "example": "# Feast: retrieve features for training (batch)\ntraining_df = store.get_historical_features(\n    entity_df=orders_df,\n    features=['customer_stats:lifetime_value', 'customer_stats:churn_score']\n).to_df()"},
]

# ─────────────────────────────────────────────
# STREAMING
# ─────────────────────────────────────────────
STREAM_STAGES = [
    {
        "label": "Produce", "icon": "📡", "sub": "events generated",
        "title": "Event Production",
        "desc": "Applications, services, IoT devices or databases emit events continuously. Each event represents a state change. Key decisions: event schema (Avro with Schema Registry), event granularity (one event per action), and key selection (determines partitioning and ordering guarantees).",
        "tools": ["Kafka Producer API", "Kafka Connect Source", "Debezium CDC", "AWS Kinesis Producer", "App SDK"],
        "concepts": ["Event schema (Avro/Protobuf)", "Topic partitioning", "Producer acks (0/1/all)", "Idempotent producer", "Schema Registry"],
        "code": '''# Kafka Avro producer with Schema Registry
from confluent_kafka.avro import AvroProducer

producer = AvroProducer({
  "bootstrap.servers":   "kafka:9092",
  "schema.registry.url": "http://schema-registry:8081",
  "acks":                "all",           # strongest durability
  "enable.idempotence":  True
}, default_value_schema=ORDER_SCHEMA)

producer.produce(
  topic="prod.orders",
  key=order["customer_id"],   # same customer → same partition
  value={"order_id": order["id"], "amount": order["total"]}
)
producer.flush()''',
    },
    {
        "label": "Broker", "icon": "📨", "sub": "Kafka / Kinesis",
        "title": "Message Broker (Kafka)",
        "desc": "The durable, distributed log that decouples producers from consumers. Kafka topics are partitioned append-only logs. Consumers read at their own pace and maintain their own offset. Data is retained for a configurable window (days to weeks), enabling replay.",
        "tools": ["Apache Kafka", "Confluent Cloud", "AWS MSK", "Azure Event Hubs", "Apache Pulsar", "Redpanda"],
        "concepts": ["Topics & partitions", "Consumer groups", "Offset management", "Replication factor", "Log compaction", "Retention policy"],
        "code": '''# Create topic with 12 partitions, RF=3, 7-day retention
kafka-topics.sh --create \
  --bootstrap-server kafka:9092 \
  --topic prod.orders \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000

# Monitor consumer group lag (critical metric)
kafka-consumer-groups.sh --describe \
  --bootstrap-server kafka:9092 \
  --group orders-processor
# LAG column = how far behind the consumer is''',
    },
    {
        "label": "Process", "icon": "⚡", "sub": "Flink / Spark SS",
        "title": "Stream Processing",
        "desc": "Continuously apply transformations, aggregations, joins and enrichments to the event stream. Apache Flink: lower latency, richer stateful operations, true streaming. Spark Structured Streaming: micro-batch, easier if team already knows Spark SQL.",
        "tools": ["Apache Flink", "Spark Structured Streaming", "ksqlDB", "Apache Beam", "Kafka Streams", "Amazon Kinesis Analytics"],
        "concepts": ["Event time vs processing time", "Watermarks", "Tumbling/sliding/session windows", "Stateful aggregation", "Exactly-once semantics", "Checkpointing"],
        "code": '''# Flink: 1-minute tumbling window aggregation with watermarks
t_env.execute_sql("""
  CREATE TABLE orders (
    order_id    STRING, customer_id STRING, amount DOUBLE,
    event_ts    TIMESTAMP(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL "10" SECOND
  ) WITH ("connector"="kafka", "topic"="prod.orders", ...)
""")

result = t_env.sql_query("""
  SELECT customer_id,
         TUMBLE_START(event_ts, INTERVAL "1" MINUTE) AS window_start,
         COUNT(*) AS order_count, SUM(amount) AS revenue
  FROM orders
  GROUP BY customer_id, TUMBLE(event_ts, INTERVAL "1" MINUTE)
""")''',
    },
    {
        "label": "Sink", "icon": "💾", "sub": "Store & serve",
        "title": "Stream Sinks & Serving",
        "desc": "Write processed stream results to one or more destinations. Real-time dashboards need a low-latency OLAP store (Apache Druid, ClickHouse, Pinot). ML feature serving needs Redis or DynamoDB. Long-term analytics lands in the Lakehouse. Multi-sink fan-out is common.",
        "tools": ["Apache Druid", "ClickHouse", "Apache Pinot", "Redis", "DynamoDB", "Delta Lake", "Elasticsearch", "Webhook / PagerDuty"],
        "concepts": ["OLAP store vs key-value", "Serving latency SLO", "Multi-sink fan-out", "Upsert semantics", "Real-time dashboard lag"],
        "code": '''# Write stream to Redis (ML features) and Delta Lake (history)
def write_features_to_redis(batch_df, epoch_id):
    for row in batch_df.collect():
        r.hset(f"cust:{row.customer_id}:features",
               mapping={"order_count_1m": row.order_count,
                        "revenue_1m": str(row.revenue)})
        r.expire(f"cust:{row.customer_id}:features", 3600)  # 1h TTL

stream_query.writeStream \
  .foreachBatch(write_features_to_redis) \
  .trigger(processingTime="10 seconds").start()

# Also sink to Delta Lake for historical replay
result_df.write.format("delta").mode("append") \
  .option("checkpointLocation", "s3://lake/checkpoints/") \
  .save("s3://lake/silver/orders_agg/")''',
    },
]

STREAM_CONCEPTS = [
    {
        "name": "Event time vs processing time",
        "desc": "Event time = when the event actually happened (in the app). Processing time = when Kafka/Flink received and processed it. Always use event time for business metrics — a late-arriving event (mobile offline sync) should count in the correct time window, not when it was received.",
        "code": '''# Flink: watermark strategy for late data (10s tolerance)
WatermarkStrategy
  .forBoundedOutOfOrderness(Duration.ofSeconds(10))
  .withTimestampAssigner((event, ts) -> event.getEventTimestamp())''',
    },
    {
        "name": "Windowing strategies",
        "desc": "Tumbling: fixed non-overlapping windows (revenue per hour). Sliding: overlapping windows (rolling 5-min avg every 1 min). Session: activity-based — window closes after a gap of inactivity (user session analytics).",
        "code": '''-- Tumbling: each event in exactly one window
TUMBLE(event_ts, INTERVAL '1' HOUR)
-- Sliding: each event in multiple overlapping windows
HOP(event_ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
-- Session: closes after 30min inactivity
SESSION(event_ts, INTERVAL '30' MINUTE)''',
    },
    {
        "name": "Exactly-once semantics",
        "desc": "Guarantees each event is processed and written to the sink exactly once — no duplicates, no data loss. Requires: idempotent producers, transactional consumers, and a sink that supports atomic writes (Delta Lake ACID, Kafka transactions).",
        "code": '''# Kafka transactional producer for exactly-once
producer.init_transactions()
producer.begin_transaction()
producer.send("output-topic", value=result)
producer.commit_transaction()  # atomic''',
    },
    {
        "name": "Backpressure handling",
        "desc": "When a downstream sink is slower than the source produces, backpressure builds up. Flink propagates backpressure upstream automatically. Solutions: increase sink parallelism, use async I/O for external lookups, add a buffer queue.",
        "code": '''# Flink async I/O for Redis enrichment (non-blocking)
class AsyncRedisEnrich(AsyncFunction):
    async def async_invoke(self, value, result_future):
        features = await redis.hgetall(f"cust:{value.customer_id}")
        result_future.complete([(*value, features)])''',
    },
]

# ─────────────────────────────────────────────
# TOOL COMPARISONS
# ─────────────────────────────────────────────
COMPARISONS = {
    "Warehouses": {
        "columns": ["Tool", "Type", "Best for", "Pricing model", "Weakness"],
        "rows": [
            ["Snowflake",   "Cloud DW",         "General analytics, many concurrent users",     "Storage + compute separate",  "Cost at very high scale"],
            ["BigQuery",    "Serverless DW",    "Ad-hoc, variable workloads, GCP-native",       "Per TB scanned (or flat)",    "No fine-grained cost control"],
            ["Redshift",    "Cloud DW (MPP)",   "AWS-native, steady predictable workloads",     "Node-hour or serverless",     "Less elastic than Snowflake"],
            ["Databricks",  "Lakehouse",        "ML + DE on same platform, Delta Lake",         "DBU (compute units)",         "Complex pricing, ops overhead"],
            ["ClickHouse",  "OLAP (self-host)", "Sub-second queries, high ingestion rate",      "Self-hosted or Cloud",        "Weak SQL compliance, ops burden"],
            ["DuckDB",      "In-process OLAP",  "Local analytics, Parquet on S3, dev/test",    "Free (open source)",          "Single-node only (no cluster)"],
        ],
    },
    "Orchestrators": {
        "columns": ["Tool", "Paradigm", "DAG definition", "Strength", "When to choose"],
        "rows": [
            ["Airflow",   "Task-centric DAGs",   "Python decorators",  "Most plugins, huge community",             "Default choice for complex pipelines"],
            ["Prefect",   "Task + flow hybrid",  "Python native",      "Dynamic DAGs, simple UI, hybrid deploy",   "Python-first teams, simpler setup"],
            ["Dagster",   "Asset-centric",       "Python + config",    "Data lineage, testing, software-defined",  "When data assets are first-class concern"],
            ["dbt Cloud", "Model-centric",       "SQL + YAML",         "Tightly coupled to dbt, great IDE",        "Pure ELT SQL transformation jobs"],
            ["Argo",      "Container-centric",   "YAML on K8s",        "Cloud-native, any language/container",     "Platform teams, Kubernetes-native orgs"],
        ],
    },
    "Streaming engines": {
        "columns": ["Tool", "Model", "Latency", "State management", "Exactly-once", "When to choose"],
        "rows": [
            ["Apache Flink",      "True streaming", "Milliseconds", "Rich (RocksDB)",    "Yes",              "Complex stateful streaming, low latency"],
            ["Spark SS",          "Micro-batch",    "Seconds",      "Moderate",          "Yes (Delta sink)", "Team knows Spark, batch + stream same code"],
            ["ksqlDB",            "SQL on Kafka",   "Seconds",      "Limited",           "At-least-once",    "Simple streaming SQL without Java/Python"],
            ["Kafka Streams",     "Library",        "Milliseconds", "Local state",       "Yes",              "Lightweight, embedded in microservices"],
            ["Kinesis Analytics", "Managed Flink",  "Seconds",      "Managed",           "Yes (Flink)",      "Fully managed Flink on AWS"],
        ],
    },
    "Ingestion tools": {
        "columns": ["Tool", "Type", "Connectors", "Managed?", "When to choose"],
        "rows": [
            ["Fivetran", "SaaS ELT",        "500+ pre-built",                    "Yes (SaaS)",     "Fast setup, willing to pay, SaaS sources"],
            ["Airbyte",  "Open source ELT", "350+ connectors",                   "Both",           "Cost-conscious, need custom connectors"],
            ["Debezium", "CDC framework",   "Postgres, MySQL, Mongo, SQL Server", "No (self-host)", "Real-time CDC at low cost"],
            ["Stitch",   "SaaS ELT",        "140+ connectors",                   "Yes (SaaS)",     "Small teams, quick wins, lower volume"],
            ["AWS DMS",  "CDC (AWS-native)","AWS + on-prem DB",                  "Yes (AWS)",      "AWS-native CDC without Kafka complexity"],
        ],
    },
    "File formats": {
        "columns": ["Format", "Type", "Compression", "Read speed", "Best for"],
        "rows": [
            ["Parquet",       "Columnar",     "Excellent (Snappy/Zstd)", "Very fast (column pruning)", "Analytics, warehouse, lake storage"],
            ["Avro",          "Row-based",    "Good",                    "Fast row reads",              "Kafka messages, CDC events, streaming"],
            ["ORC",           "Columnar",     "Excellent",               "Very fast",                   "Hive/Hadoop ecosystems (legacy)"],
            ["JSON",          "Row text",     "None (compressible)",     "Slow",                        "Raw ingestion, APIs, flexibility needed"],
            ["CSV",           "Row text",     "None",                    "Slow",                        "Simple exports, non-technical consumers"],
            ["Delta/Iceberg", "Table format", "Parquet underneath",      "Fast + ACID",                 "Lakehouse ACID tables with time travel"],
        ],
    },
    "Table formats": {
        "columns": ["Format", "ACID", "Time travel", "Schema evolution", "Best with"],
        "rows": [
            ["Delta Lake",     "Yes (full)",         "Yes (time travel)", "Yes (additive + rename)", "Databricks, Spark, PySpark"],
            ["Apache Iceberg", "Yes (full)",         "Yes (snapshot)",    "Yes (additive, rename, drop)", "Snowflake, BigQuery, Spark, Trino"],
            ["Apache Hudi",    "Yes (record-level)", "Yes (timeline)",    "Limited",                  "Spark, high-frequency upserts"],
            ["Hive (classic)", "No",                 "No",                "Add columns only",         "Legacy Hadoop — avoid for new builds"],
        ],
    },
}

# ─────────────────────────────────────────────
# REAL-WORLD CASE STUDIES
# ─────────────────────────────────────────────
REAL_WORLD = [
    {"company": "Uber",     "sector": "Ride-sharing",              "stack": "Kafka · Flink · Hudi · Presto · Pinot",              "desc": "Uber processes 1M+ events/sec. Built Apache Hudi for record-level upserts on S3 (driver location, trip state). Apache Pinot for sub-second real-time analytics (surge pricing). Flink for fraud detection and dynamic pricing.", "learn": "Hudi and Pinot were both invented at Uber and open-sourced. Real-time upserts on lake at petabyte scale."},
    {"company": "Airbnb",   "sector": "Marketplace",               "stack": "Kafka · Spark · Delta Lake · Airflow · Superset",    "desc": "Airbnb invented Apache Airflow (open-sourced 2015). Their Minerva metric layer defines company-wide metrics as code. Uses Delta Lake for ACID on S3. Druid for real-time host/guest analytics dashboards.", "learn": "Metric consistency at scale: 'revenue' defined once, used everywhere. Airflow DAGs for all ETL."},
    {"company": "LinkedIn", "sector": "Social / B2B",              "stack": "Kafka · Samza · Pinot · Espresso · Venice",          "desc": "LinkedIn invented Apache Kafka (open-sourced 2011) and Apache Samza (stream processing). Pinot for real-time member analytics. Venice for feature serving. Processes 7 trillion events per day.", "learn": "The origin of the modern streaming stack. Every tool they built got open-sourced and is now industry standard."},
    {"company": "Netflix",  "sector": "Streaming / Entertainment", "stack": "Kafka · Spark · Iceberg · Flink · Metaflow · Druid",  "desc": "Netflix adopted Apache Iceberg at massive scale — PBs of data across thousands of tables. Metaflow (open-sourced) for ML pipelines. Maestro for job orchestration. Druid for real-time A/B test analytics.", "learn": "Iceberg adoption led the industry pivot from Hive to Iceberg. Their blog posts on Iceberg migration are essential reading."},
    {"company": "Stripe",   "sector": "Fintech / Payments",        "stack": "Kafka · Spark · Iceberg · dbt · Airflow · Snowflake", "desc": "Stripe processes billions of financial events. Kafka for event streaming, Iceberg on S3 for the lake, Snowflake for analytics serving. Strict data contracts and schema validation for every pipeline — regulatory compliance requires it.", "learn": "In fintech: every pipeline must be auditable, every schema change reviewed, every PII field masked. Data contracts are non-negotiable."},
    {"company": "DoorDash", "sector": "Delivery / Marketplace",    "stack": "Kafka · Flink · BigQuery · dbt · Airflow · Snowflake", "desc": "BigQuery for scalable ad-hoc analytics, Flink for real-time ETA and routing decisions, dbt for transformation layer, Airflow for orchestration. Migrated from a monolithic Redshift to this modern stack.", "learn": "Classic growth migration story. Redshift hit limits → BigQuery + dbt + Flink. Start simple, scale tools when pain is real."},
]

# ─────────────────────────────────────────────
# INTERVIEW Q&A
# ─────────────────────────────────────────────
INTERVIEW_QA = [
    {
        "q": "What is the difference between ETL and ELT, and when do you choose each?",
        "cat": "Fundamentals",
        "ans": "ETL: transform before loading — data is clean before hitting the destination. Needed when the destination can't handle raw data. ELT: load raw first, transform inside the warehouse using SQL/dbt. Modern default — warehouses (Snowflake, BigQuery) are powerful enough to transform at scale, and raw data is preserved for reprocessing. Choose ELT unless you have a specific reason not to.",
        "code": "# ETL (old): transform in Python, then load\nclean_df = raw_df.filter(...).agg(...)\nclean_df.to_sql('orders', engine)\n\n# ELT (modern): load raw, transform in warehouse via dbt\nCOPY INTO raw.orders FROM @stage/orders.parquet;\n-- dbt model transforms inside Snowflake",
    },
    {
        "q": "Explain idempotency in data pipelines. How do you achieve it?",
        "cat": "Fundamentals",
        "ans": "An idempotent pipeline produces the same result whether run once or ten times. Critical for retries and backfills. Achieved by: (1) MERGE/UPSERT instead of INSERT, (2) overwrite specific partitions on the lake instead of appending, (3) storing watermarks atomically with data.",
        "code": "-- MERGE ensures re-run is safe (no duplicates)\nMERGE INTO fct_orders t USING staging s ON t.order_id = s.order_id\nWHEN MATCHED    THEN UPDATE SET status = s.status\nWHEN NOT MATCHED THEN INSERT (order_id, ...) VALUES (s.order_id, ...)",
    },
    {
        "q": "What is Change Data Capture (CDC) and how does Debezium work?",
        "cat": "Fundamentals",
        "ans": "CDC captures every row-level mutation (INSERT/UPDATE/DELETE) from a source database by reading its transaction log — WAL in Postgres, binlog in MySQL. Zero impact on source DB. Debezium runs as a Kafka Connect connector: reads the WAL, converts each change into an Avro/JSON event, publishes to a Kafka topic.",
        "code": '# Debezium event structure\n{"op": "u",  # c=create, u=update, d=delete\n "before": {"status": "pending"},\n "after":  {"status": "shipped"},\n "source": {"table": "orders", "ts_ms": 1709123456789}}',
    },
    {
        "q": "Design a pipeline that ingests 10M orders/day from Postgres into a data warehouse.",
        "cat": "System Design",
        "ans": "1. Ingestion: Debezium CDC on Postgres replica → Kafka (12 partitions). 2. Raw landing: Kafka Connect S3 sink writes hourly Parquet files partitioned by date. 3. Transformation: Airflow DAG triggers dbt run hourly. 4. Quality: Soda checks row count, null rate, referential integrity. 5. Serving: Snowflake materialized view for BI. Total latency: ~35 minutes.",
        "code": "# Rough Airflow DAG structure\ningest_cdc >> land_raw_s3 >> trigger_dbt >> run_soda_checks >> refresh_mv",
    },
    {
        "q": "What is the difference between a Data Lake, Data Warehouse, and Lakehouse?",
        "cat": "Architecture",
        "ans": "Data Lake: raw files (JSON, Parquet, CSV) on cheap object storage (S3). Schema-on-read. Flexible but no ACID. Data Warehouse: structured, schema-on-write, optimised for SQL analytics (Snowflake, BigQuery). Fast queries but expensive storage. Lakehouse: best of both — raw data on cheap object storage + ACID transactions + SQL via open table formats (Delta, Iceberg).",
        "code": "# Lakehouse query: Iceberg table on S3 via Spark\nspark.read.format('iceberg') \\\n  .load('s3://lake/silver/orders') \\\n  .filter(\"ordered_at >= '2024-01-01'\") \\\n  .groupBy('region') \\\n  .agg(sum('amount').alias('revenue')) \\\n  .show()",
    },
    {
        "q": "Explain SCD Type 2. When and how do you implement it?",
        "cat": "SQL & Modeling",
        "ans": "SCD2 preserves history when a dimension attribute changes — rather than overwriting, add a new row with effective_from, effective_to and is_current flag. Use when business needs point-in-time accuracy: 'what was the customer's segment at time of purchase?' Implement with dbt snapshots (built-in SCD2) or MERGE in SQL.",
        "code": "-- dbt snapshot for SCD2 customer dimension\n{% snapshot dim_customers_snapshot %}\n{{ config(target_schema='snapshots', unique_key='customer_id',\n          strategy='timestamp', updated_at='updated_at') }}\nSELECT * FROM {{ source('raw','customers') }}\n{% endsnapshot %}",
    },
    {
        "q": "How do you ensure data quality in a production pipeline?",
        "cat": "Fundamentals",
        "ans": "Three layers: (1) Schema validation at ingestion — reject or quarantine malformed records. (2) dbt tests after transformation — not_null, unique, accepted_values, referential integrity. (3) Soda/Great Expectations post-load — freshness, row count, null rate, distribution anomalies. Block downstream promotion if checks fail.",
        "code": "# soda check file — runs after every dbt build\nchecks for fct_orders:\n  - row_count > 10000\n  - missing_count(order_id) = 0\n  - duplicate_count(order_id) = 0\n  - min(total_amount_usd) >= 0\n  - freshness(ordered_at) < 6h",
    },
    {
        "q": "How do you handle schema evolution in streaming pipelines?",
        "cat": "Streaming",
        "ans": "Use Avro with Confluent Schema Registry. Register schemas with backward compatibility — new schemas can read old data. For Flink/Spark: use schema merging on Delta/Iceberg so new columns are additive without breaking existing queries. Alert on unexpected schema changes at ingestion via a schema validation step.",
        "code": "# Schema Registry backward compatibility check\nPOST /subjects/orders-value/versions\n{\"schema\": \"...\", \"schemaType\": \"AVRO\"}\n# Registry enforces backward compatibility:\n# old consumers reading new messages get defaults for missing fields",
    },
    {
        "q": "What is partitioning and why is it critical for performance?",
        "cat": "SQL & Modeling",
        "ans": "Partitioning divides a large table into logical segments (usually by date) so queries only scan the relevant partition. A query with WHERE date = '2024-03-15' on a partitioned table skips all other dates — 100x+ speedup and cost reduction. Rule: always partition by the most common filter column.",
        "code": "-- BigQuery: partition by date, cluster by region + status\nCREATE TABLE analytics.fct_orders\nPARTITION BY DATE(ordered_at)\nCLUSTER BY region, status\nOPTIONS (partition_expiration_days = 730);",
    },
    {
        "q": "Design a real-time fraud detection pipeline.",
        "cat": "System Design",
        "ans": "1. Events: payment service emits transaction events to Kafka topic payments.raw. 2. Stream processing: Flink job enriches each transaction with historical features from Redis feature store, applies ML model (ONNX runtime in Flink), emits fraud score within 50ms. 3. Decision: if score > 0.9, publish to payments.flagged → webhook to fraud team. 4. Sink: all events + scores → Delta Lake for ML retraining.",
        "code": "# Flink fraud detection job (simplified)\nclass FraudScorer(FlatMapFunction):\n    def open(self, ctx):\n        self.model = onnxruntime.InferenceSession('fraud_model.onnx')\n        self.redis  = redis.Redis(host='feature-store.internal')\n    def flat_map(self, txn):\n        features = self.redis.hgetall(f'cust:{txn.customer_id}:features')\n        score    = self.model.run(None, build_input(txn, features))[0][0]\n        yield {**txn, 'fraud_score': float(score), 'flagged': score > 0.9}",
    },
]

IQ_CATEGORIES = ["All", "Fundamentals", "Architecture", "SQL & Modeling", "Streaming", "System Design"]

# ─────────────────────────────────────────────
# CAREER ROADMAP
# ─────────────────────────────────────────────
ROLES = [
    {
        "id": "junior", "name": "Junior DE", "level": "0–2 yrs",
        "desc": "Builds and maintains existing pipelines, writes SQL transformations, learns the stack, fixes data quality issues under senior guidance.",
        "skills": [
            {"name": "SQL (intermediate)", "pct": 70},
            {"name": "Python basics",       "pct": 65},
            {"name": "Git / version control","pct": 70},
            {"name": "dbt basics",          "pct": 50},
            {"name": "Airflow basics",      "pct": 40},
            {"name": "Cloud fundamentals",  "pct": 40},
        ],
        "techs": ["SQL", "Python", "dbt", "Airflow", "S3/GCS", "Snowflake/BigQuery"],
        "next": "Learn Spark, deepen dbt, understand CDC, build your first end-to-end pipeline independently.",
    },
    {
        "id": "mid", "name": "Mid-level DE", "level": "2–5 yrs",
        "desc": "Designs and owns full pipelines end-to-end. Makes tooling choices. Implements data quality frameworks. Comfortable with distributed systems concepts.",
        "skills": [
            {"name": "SQL (advanced)",       "pct": 85},
            {"name": "PySpark / Spark SQL",  "pct": 75},
            {"name": "dbt (advanced)",       "pct": 80},
            {"name": "Airflow / Prefect",    "pct": 75},
            {"name": "Kafka basics",         "pct": 60},
            {"name": "Data modeling",        "pct": 70},
            {"name": "Cloud services depth", "pct": 65},
        ],
        "techs": ["PySpark", "Kafka", "Delta Lake", "dbt", "Airflow", "Snowflake", "Terraform"],
        "next": "Develop streaming expertise, architect full platforms, mentor juniors, build data contracts and observability.",
    },
    {
        "id": "senior", "name": "Senior DE", "level": "5–8 yrs",
        "desc": "Architects data platforms. Drives technical decisions. Defines standards (data contracts, SLAs, quality frameworks). Deep streaming and distributed systems knowledge.",
        "skills": [
            {"name": "Streaming (Flink/Kafka)", "pct": 85},
            {"name": "Platform architecture",   "pct": 85},
            {"name": "Data modeling depth",     "pct": 90},
            {"name": "Distributed systems",     "pct": 80},
            {"name": "Infra as code",           "pct": 75},
            {"name": "Cross-team influence",    "pct": 80},
        ],
        "techs": ["Flink", "Kafka", "Iceberg", "Terraform", "DataHub", "Spark", "Kubernetes"],
        "next": "Staff / Principal path: drive org-wide data strategy, define data mesh adoption, build platform teams.",
    },
    {
        "id": "staff", "name": "Staff / Principal DE", "level": "8+ yrs",
        "desc": "Defines data strategy for the entire organisation. Drives architecture decisions across teams. Bridges engineering and business leadership.",
        "skills": [
            {"name": "Org-wide architecture",    "pct": 95},
            {"name": "Engineering leadership",   "pct": 90},
            {"name": "Product / business sense", "pct": 85},
            {"name": "Full stack depth",         "pct": 90},
            {"name": "Vendor evaluation",        "pct": 85},
            {"name": "Mentorship at scale",      "pct": 90},
        ],
        "techs": ["Data Mesh patterns", "Platform engineering", "OpenLineage", "DataHub", "FinOps", "All cloud platforms"],
        "next": "Engineering Manager path OR Individual Contributor Fellow. Focus on multi-year roadmaps and org design.",
    },
]

LEARN_PATH = [
    {"step": 1, "title": "SQL mastery",                         "items": ["Window functions (ROW_NUMBER, LAG, LEAD, DENSE_RANK)", "CTEs and recursive CTEs", "Query optimisation (EXPLAIN, indexing, partitioning)", "Star schema design and slowly changing dimensions"]},
    {"step": 2, "title": "Python for data engineering",         "items": ["Pandas, data validation, file I/O (Parquet, Avro)", "Writing reusable pipeline functions with proper error handling", "Working with APIs: pagination, retries, rate limiting", "Type hints, logging, unit testing your pipelines"]},
    {"step": 3, "title": "dbt + transformation layer",          "items": ["Sources, staging models, marts structure", "Incremental models and backfill strategy", "dbt tests: not_null, unique, accepted_values, custom", "Macros, packages (dbt_utils, dbt_expectations)"]},
    {"step": 4, "title": "Orchestration (Airflow)",             "items": ["DAG design, operators, sensors, hooks", "Idempotent task design with retries", "XCom for task communication, Variables, Connections", "SLA alerts, monitoring, backfill patterns"]},
    {"step": 5, "title": "Cloud platform depth (pick one)",     "items": ["AWS: S3 + Glue Catalog + Redshift + MSK + EMR", "GCP: GCS + BigQuery + Pub/Sub + Dataflow + Composer", "Azure: ADLS + Synapse + Event Hubs + Data Factory + Databricks"]},
    {"step": 6, "title": "Spark & distributed processing",      "items": ["PySpark DataFrame API, SQL, UDFs", "Partitioning, bucketing, broadcast joins", "Spark Structured Streaming basics", "Running on EMR / Dataproc / Databricks"]},
    {"step": 7, "title": "Streaming & real-time (Kafka+Flink)", "items": ["Kafka: topics, partitions, consumer groups, offsets", "Avro + Schema Registry for event schemas", "Flink or Spark Structured Streaming for stateful processing", "Watermarks, windows, exactly-once semantics"]},
    {"step": 8, "title": "Data quality & observability",        "items": ["Great Expectations or Soda for automated checks", "Data lineage via OpenLineage / DataHub", "Alerting on SLA breaches, freshness, anomalies", "Building a data observability culture on your team"]},
]

# ─────────────────────────────────────────────
# ANTI-PATTERNS
# ─────────────────────────────────────────────
ANTI_PATTERNS = [
    {
        "name": "The God Pipeline", "tagline": "one DAG that does everything",
        "desc": "A single Airflow DAG or Spark job that ingests, validates, transforms, aggregates, and writes to 3 destinations — 800 lines of code, 40 tasks, no modularity. When it fails, you don't know where. When you need to change one step, you risk breaking everything.",
        "fix": "Decompose into single-responsibility pipelines. One DAG per logical concern. Use ExternalTaskSensor to chain them. Each pipeline can be debugged, rerun, and owned independently.",
        "code": "# BAD: one giant DAG does everything\ndef run_everything():\n    data = extract(); transformed = transform(data)\n    load_to_snowflake(transformed); load_to_s3(transformed)\n\n# GOOD: separate pipelines with clear ownership\n# ingest_dag >> transform_dag >> serve_dag (ExternalTaskSensor)",
    },
    {
        "name": "No Raw Layer (Transform-on-Ingest)", "tagline": "transforming before landing raw",
        "desc": "Applying business logic, filters or aggregations during ingestion so raw data is never preserved. When requirements change or a bug is found, there is nothing to reprocess from.",
        "fix": "Always land raw data unchanged. Apply transformations in a separate downstream step. The raw layer is your reprocessing safety net — never skip it.",
        "code": "# BAD: transform during ingest (raw lost forever)\nrecords = [apply_business_logic(r) for r in fetch_api()]\nload_to_warehouse(records)\n\n# GOOD: raw first, transform separately\nwrite_parquet_to_s3(fetch_api(), path='s3://lake/raw/orders/')",
    },
    {
        "name": "Polling OLTP in Production", "tagline": "SELECT * from prod DB every 5 minutes",
        "desc": "Running analytical queries directly against the production Postgres or MySQL database. This causes: query competition with live traffic, risk of replication lag on slaves, and risk of long-running analytics queries locking tables.",
        "fix": "Always use a read replica for any polling. Better: use CDC (Debezium) to stream changes without any query load. Never run Spark or Airflow jobs against your production OLTP DB.",
        "code": "# BAD: polling prod DB\ndf = spark.read.jdbc(url='jdbc:postgresql://prod-db:5432/orders', ...)\n\n# GOOD: read from replica\ndf = spark.read.jdbc(url='jdbc:postgresql://read-replica:5432/orders', ...)\n# BEST: Debezium CDC — zero query load on source",
    },
    {
        "name": "Non-Idempotent Pipelines", "tagline": "INSERT without dedup = duplicates on retry",
        "desc": "Using INSERT INTO ... SELECT * without any deduplication logic. When the pipeline retries after a transient failure, the same records are inserted again. Result: duplicate rows in your fact table, wrong revenue numbers, angry analysts.",
        "fix": "Always use MERGE/UPSERT on a natural unique key. On the lake: overwrite a partition cleanly rather than appending. Test by running twice on the same input and checking for duplicates.",
        "code": "# BAD: append without dedup — duplicates on retry\nINSERT INTO fct_orders SELECT * FROM staging.new_orders;\n\n# GOOD: MERGE on unique key — safe to re-run\nMERGE INTO fct_orders t USING staging.new_orders s\n  ON t.order_id = s.order_id\n  WHEN MATCHED    THEN UPDATE SET ...\n  WHEN NOT MATCHED THEN INSERT ...",
    },
    {
        "name": "Ignoring Small Files Problem", "tagline": "millions of 10KB Parquet files on S3",
        "desc": "Streaming jobs or high-frequency batch jobs writing many small files to S3/GCS. Spark spends more time opening file handles than reading data. Athena/Presto query time explodes. One Flink job writing every 10 seconds = 8,640 files/day per partition.",
        "fix": "Compact small files regularly using Delta Lake OPTIMIZE, Iceberg rewrite_data_files, or Spark coalesce() before writing. Set trigger intervals to write files of 128MB–512MB target size.",
        "code": "-- Delta Lake: compact small files\nOPTIMIZE delta.`s3://lake/silver/orders` ZORDER BY (customer_id);\n\n# Spark: coalesce before writing\ndf.coalesce(8).write.mode('overwrite').parquet('s3://...')",
    },
    {
        "name": "No Monitoring or Alerting", "tagline": "stakeholders report failures before you do",
        "desc": "Pipelines complete silently. No row count logs, no freshness checks, no SLA alerts. The analyst finds the dashboard is stale from 6 hours ago — because the Airflow job failed silently at 2AM and nobody noticed.",
        "fix": "Log row counts and latency at every stage. Set SLA on Airflow tasks. Add freshness checks in Soda/GX after every run. Alert to Slack/PagerDuty on failure. Build a pipeline health dashboard.",
        "code": "# Airflow SLA miss callback\ndef sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):\n    send_slack_alert(f'SLA missed: {task_list} in DAG {dag.dag_id}')\n\nwith DAG(..., sla_miss_callback=sla_miss_callback) as dag:\n    transform = PythonOperator(sla=timedelta(hours=2), ...)",
    },
]

# ─────────────────────────────────────────────
# PIPELINE CHECKLIST
# ─────────────────────────────────────────────
CHECKLIST = [
    {
        "section": "Ingestion", "items": [
            "Source data lands in a raw layer unchanged (no transform on ingest)",
            "Ingestion is idempotent (re-runnable without duplicates)",
            "Watermark / checkpoint is stored and updated atomically",
            "Row count and file size are logged after every load",
            "Schema of incoming data is validated and alerts exist for drift",
            "PII fields are identified and masked or encrypted at landing",
        ],
    },
    {
        "section": "Transformation", "items": [
            "dbt models use incremental strategy with proper unique_key",
            "Every model has at least: not_null + unique tests on primary key",
            "SCD2 is implemented for dimensions requiring history",
            "Deduplication logic is explicit (ROW_NUMBER or MERGE)",
            "Null handling is explicit (COALESCE, IS NOT NULL filters)",
            "Transformation code is version-controlled and peer-reviewed",
        ],
    },
    {
        "section": "Orchestration", "items": [
            "DAG has retries + retry_delay + exponential backoff",
            "catchup=False is set (or intentional if catchup needed)",
            "SLA is defined and sla_miss_callback alerts to Slack/PagerDuty",
            "Tasks are idempotent — re-running any task is safe",
            "Backfill has been tested in staging before production deployment",
            "DAG has owner, tags, and doc_md documentation",
        ],
    },
    {
        "section": "Data Quality", "items": [
            "Automated quality checks run after every pipeline execution",
            "Freshness check: data is expected to arrive within defined window",
            "Row count is compared to expected range or previous day",
            "Referential integrity checks pass (no orphaned FK rows)",
            "Quality check failures block downstream pipeline stages",
            "Quality metrics are tracked over time (dashboarded)",
        ],
    },
    {
        "section": "Security & Compliance", "items": [
            "PII columns are masked in serving layer (column masking policy)",
            "Row-level security applied where needed (region, tenant)",
            "All data access is logged for audit trail",
            "Right-to-erasure workflow exists for GDPR/CCPA compliance",
            "Secrets are in a vault (AWS Secrets Manager, HashiCorp Vault) — not in code",
            "Data retention policies are implemented and automated",
        ],
    },
    {
        "section": "Performance & Cost", "items": [
            "Tables are partitioned by date (and clustered where needed)",
            "Small files are compacted regularly (OPTIMIZE on Delta/Iceberg)",
            "Warehouse compute is sized appropriately — not over-provisioned",
            "Query costs are monitored (BigQuery slot usage, Snowflake credits)",
            "Spark jobs have appropriate executor sizing (no OOM or idle cores)",
            "Unused intermediate tables / partitions are cleaned up on schedule",
        ],
    },
]
