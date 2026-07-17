# 3. Incremental Data Processing

# Development and Ingestion

## Structured Streaming

Structured Streaming is basically **“Spark SQL for streams”**: you write a normal DataFrame query, and Spark runs it *incrementally* as new data arrives instead of just once on a static dataset. [downloads.apache.org+2apache.github.io+2](https://downloads.apache.org/spark/docs/3.1.1/structured-streaming-programming-guide.html?utm_source=chatgpt.com)

Very high level:

- A **stream** is modeled as a **table that keeps getting new rows**.
- You define a query (`select`, `where`, `groupBy`, `join`, etc.) just like batch.
- The engine **re-runs the query on new data only**, updating the result.
- It’s **fault-tolerant** and supports **exactly-once** processing using checkpoints. [downloads.apache.org](https://downloads.apache.org/spark/docs/3.1.1/structured-streaming-programming-guide.html?utm_source=chatgpt.com)

---

### Core concepts you should remember for the exam

- **Streaming DataFrame**: created with `readStream` instead of `read`.
- **Source**: where the stream comes from (files, Kafka, etc.).
- **Sink**: where results go (console, Delta tables, Kafka, etc.).
- **Output mode**: `append`, `update`, or `complete`.
- **Trigger**: how often to process new data (default micro-batch, fixed interval, `availableNow`, etc.). [Medium+1](https://medium.com/towards-data-engineering/understanding-triggers-with-databricks-spark-structured-streaming-default-processingtime-and-be39b68ea6d6?utm_source=chatgpt.com)
- **Checkpoint**: directory where Spark stores progress/state → required for reliability.

### Watermarks

**1. The problem watermarks solve**

In real streams:

- Events arrive **late** or **out of order** (network delays, retries, batched sends, etc.).
- If Spark waited forever for “maybe there’s one more late event”, then:
    - **Window aggregations** (`groupBy(window(ts, ...))`) would keep state forever.
    - Memory/state would **grow without bound** → slow, possibly crash.

You need a rule to say:

> “After some point, I’m OK to finalize results for old time ranges and drop their state, even if a very-late event appears later.”
> 

That rule is the **watermark**.

**2. What *is* a watermark?**

A **watermark** is a threshold on **event time lateness**.

In code:

```python
events_with_wm = events_df.withWatermark("ts", "10 minutes")
```

This means (conceptually):

> Spark will assume that events can arrive up to 10 minutes late based on ts.
> 
> 
> Once it has seen a maximum event time `T_max`, any event with `ts < T_max - 10 minutes` is considered **too late**.
> 
- For **windowed aggregations** and **stream–stream joins**, Spark can:
    - **Emit final results** for windows whose end time is older than the watermark.
    - **Evict state** for those windows from memory.

Late events older than the watermark are typically **dropped** (ignored for that aggregation/join).

**4. Key points to remember (Databricks / exam mindset)**

**One sentence for the exam**

> **Structured Streaming is a continuously running DataFrame query that incrementally processes new data from a streaming source and writes the results to a sink.**
> 

I think that's an excellent high-level definition to remember. It captures the essence of Structured Streaming in one sentence.

- Watermarks are defined on a **timestamp column** representing **event time**.
- They are meaningful only for **stateful ops**: windowed aggregations, stream–stream joins, some dedup.
- They control **how long Spark waits for late data** and **when old state can be evicted**.
- Late data **older than the watermark** is generally **ignored** for those stateful ops.+

### Example 1 – Simple file stream → console

Imagine JSON logs landing continuously in `/mnt/events/`.

### 1. Source: Create the streaming DataFrame

```python
from pyspark.sql.functions import col

# Streaming read from a directory of JSON files
events_df = (spark.readStream
    .format("json")
    .schema("user_id STRING, event_type STRING, ts TIMESTAMP")  # recommended: provide schema
    .load("/mnt/events/"))
```

### 2. Transformation: Simple transformation: filter and select columns

```jsx
# Simple transformation: filter and select columns
clicks_df = (events_df
    .filter(col("event_type") == "click")
    .select("user_id", "ts"))
```

### 3. Sink: Write the stream to the console (for dev/debug)

```python
query = (clicks_df.writeStream
    .format("console")                  # developer-only sink
    .outputMode("append")               # only new rows
    .option("truncate", False)
    .start())
```

- Spark will continually print new `click` rows as files appear under `/mnt/events/`.
- In production you replace `console` with something like Delta or Kafka.

To stop it in Databricks: `query.stop()`.

### Example 2 – Streaming aggregation with watermark → Delta table

Now suppose you want **near real-time per-minute counts** of clicks, with late data handling.

### Define aggregation with event-time window + watermark

```python
from pyspark.sql.functions import window, count

# Reuse events_df from above (Reuse the Source)

# Aggegating in the transformation step
agg_df = (events_df
    .withWatermark("ts", "10 minutes")          # late events allowed up to 10 min
    .groupBy(
        window(col("ts"), "1 minute"),          # 1-minute tumbling windows
        col("event_type")
    )
    .agg(count("*").alias("cnt")))

```

### Write aggregation to a Delta table

```python
output_path = "dbfs:/mnt/analytics/event_counts"
checkpoint_path = "dbfs:/mnt/checkpoints/event_counts"

query_agg = (agg_df.writeStream
    .format("delta")
    .outputMode("append")               # each finished window produces new rows
    .option("checkpointLocation", checkpoint_path)
    .start(output_path))

```

Then you can query the results like any Delta table:

```python
event_counts = spark.read.format("delta").load(output_path)
display(event_counts.orderBy("window"))

```

Key points this example illustrates (very exam-relevant):

- **`withWatermark`**: tells Spark how long to wait for late data before finalizing window results.
- **Windowed aggregations** on event time.
- **Delta as a streaming sink** with `checkpointLocation` → exactly-once semantics. [downloads.apache.org+1](https://downloads.apache.org/spark/docs/3.1.1/structured-streaming-programming-guide.html?utm_source=chatgpt.com)

### Example 3 – Stream (events) joined with static dimension

Very common pattern: enrich a stream with a static table (e.g., user attributes).

```python
# Static dimension table
dim_users = spark.table("dim_users")   # batch DataFrame, not streaming

# Join streaming events with static dimension
enriched_df = (events_df
    .join(dim_users, on="user_id", how="left"))

query_enriched = (enriched_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/checkpoints/enriched_events")
    .start("dbfs:/mnt/analytics/enriched_events"))

```

Spark automatically treats this as a **stream–static join**, which is supported and common in pipelines. [downloads.apache.org+1](https://downloads.apache.org/spark/docs/3.1.1/structured-streaming-programming-guide.html?utm_source=chatgpt.com)

### Structured Streaming Hands On

## Incremental Data Ingestion

In Databricks, **incremental data ingestion** means *only* loading **new or changed data** from cloud storage into your tables (usually Delta), instead of re-reading everything every time. This reduces cost, speeds up pipelines, and is the standard pattern for Bronze → Silver layers.

### Incremental ingestion in Databricks (quick recap)

Key ideas:

- Data lands in **cloud object storage** (S3, ADLS, GCS).
- You want a **Delta table** that always has the latest data.
- Instead of `INSERT OVERWRITE` or full scans, you:
    - Track what has already been loaded.
    - Only process **new files** or **files changed after a certain timestamp**.
- Incrementality is implemented through:
    - **Metadata** about processed files (COPY INTO, Auto Loader checkpoints).
    - **Streaming semantics** (Auto Loader).
    - **Idempotence**: you can safely rerun without duplicating rows.

### Incremental ingestion with `COPY INTO`

### 2.1. What `COPY INTO` is

`COPY INTO` is a **SQL command** that loads data from a file location into a Delta (or other) table. It is **re-triable and idempotent**: once a file has been loaded, it is skipped on later runs. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/?utm_source=chatgpt.com)

- Databricks explicitly recommends `COPY INTO` for **incremental and bulk loading** when you have **thousands of files**;
- Auto Loader is recommended for more advanced scenarios. [Databricks Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/tutorial-notebook?utm_source=chatgpt.com)

Typical use: schedule a job that runs `COPY INTO` every X minutes/hours over a landing folder.

### 2.2. How incrementality works in `COPY INTO`

- The command keeps **metadata of loaded files** (in the Delta table’s history / log).
- On each run, it:
    - Lists files at the source path.
    - Skips any file it has previously ingested.
- You can **force reload** files with `COPY_OPTIONS ('force' = 'true')`, which turns off idempotency for that run. [Databricks Documentation+1](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into?utm_source=chatgpt.com)

You can also use `modifiedAfter` to only consider files changed after a certain timestamp. [Databricks Documentation+1](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into?utm_source=chatgpt.com)

### 2.3. Basic `COPY INTO` example

**SQL:**

```sql
-- Create the target Delta table (Bronze)
CREATE TABLE IF NOT EXISTS bronze.sales_raw (
  order_id      STRING,
  customer_id   STRING,
  order_ts      TIMESTAMP,
  amount        DOUBLE
)
USING DELTA;

-- Incremental load from cloud storage
COPY INTO bronze.sales_raw
FROM 's3://my-landing-bucket/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true' 
)
COPY_OPTIONS (
  'mergeSchema' = 'true'
);
```

Notes:

- **Run this repeatedly** (job/Workflow, Lakeflow Job, etc.). Each run only loads **new** files because of idempotency.
- `mergeSchema = 'true'` allows **schema evolution** if new columns appear in the incoming data. [Microsoft Learn+1](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into?utm_source=chatgpt.com)

### 2.4. Important options for incremental use

Some key options (see docs for full list): [Databricks Documentation+1](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into?utm_source=chatgpt.com)

- `COPY_OPTIONS('force' = 'false' | 'true')`
    - `false`: default, **idempotent**, skip already loaded files.
    - `true`: reload everything (e.g., backfill or fix bad load).
- `COPY_OPTIONS('mergeSchema' = 'true')`
    - Allow schema evolution as new columns appear.
- `ignoreMissingFiles`, `ignoreCorruptFiles`
    - Control how to handle missing / corrupt files.
- `VALIDATE n ROWS`
    - Validate sample rows before committing load (useful for catching issues).
- `FILES` / `PATTERN`
    - To narrow which files you load (e.g., `PATTERN = '.*2025-12-.*.csv'`).

### 2.5. When `COPY INTO` is a good fit

Use `COPY INTO` when:

- You want **simple, SQL-based, batch incremental loads**.
- You can tolerate a **directory listing** every run (number of files is not insane).
- You are doing **daily/hourly** refreshes rather than low-latency streaming.
- You want easy integration with tools like **dbt** (there’s a `databricks_copy_into` macro). [GitHub](https://github.com/databricks/dbt-databricks/blob/main/docs/databricks-copy-into-macro-aws.md?utm_source=chatgpt.com)

Limitations:

- Not a true streaming source.
- For **millions of small files** and continuous ingestion, listing becomes expensive → Auto Loader is better.

### Auto-Loader

### 3.1. What Auto Loader is

**Auto Loader** is a **Structured Streaming source (`cloudFiles`)** that *incrementally and efficiently* processes new files as they arrive in cloud storage. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/?utm_source=chatgpt.com)

Given an input directory, Auto Loader:

- Can **process existing files once** (initial backfill).
- Then continuously or periodically ingest **only new files**.
- Uses **checkpoints** to remember progress.
- Is recommended by Databricks for **advanced incremental / streaming ingestion**, often together with Delta Live Tables. [Databricks+1](https://www.databricks.com/resources/demos/videos/ingestion/ingestion?utm_source=chatgpt.com)

---

### 3.2. How Auto Loader works (high level)

Auto Loader uses a special source format: **`cloudFiles`**. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/?utm_source=chatgpt.com)

Core pieces:

- `cloudFiles.format` → file type (`json`, `csv`, `parquet`, etc.). [Microsoft Learn](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options?utm_source=chatgpt.com)
- **Checkpoint location** → where Structured Streaming stores progress.
- **Schema location** → where it stores discovered schema and evolution metadata. [Databricks Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema?utm_source=chatgpt.com)
- **File discovery mode**:
    - **Directory listing mode**: lists paths to find new files.
    - **File notification mode**: uses object storage events & queues (faster, more scalable). Controlled by `cloudFiles.useNotifications` or managed file events options. [Microsoft Learn+1](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options?utm_source=chatgpt.com)

Because it’s built on **Structured Streaming**, you can run:

- **Continuous** mode (near real-time).
- **`trigger(availableNow=True)` / `trigger(once=True)`** to process in micro-batch but still incremental.

---

### 3.3. Auto Loader example in Python

```python
from pyspark.sql.functions import *

input_path = "s3://my-landing-bucket/sales/"
checkpoint_path = "s3://my-checkpoints/bronze_sales/"
schema_path = "s3://my-schema-locations/bronze_sales/"

(
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)  # where schema & evolution are tracked
    .option("cloudFiles.inferColumnTypes", "true")     # optional, for better types
    .load(input_path)
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")                      # allow schema evolution in the target
    .trigger(availableNow=True)                        # batch-like but incremental
    .table("bronze.sales_raw")
)

```

SQL version (Lakehouse / Lakeflow jobs) is similar but using `READ_STREAM("cloudFiles", ...)`.

---

### 3.4. Schema inference & evolution

Auto Loader can: [Databricks Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema?utm_source=chatgpt.com)

- Automatically **infer schema** from the incoming files (`cloudFiles.schemaLocation`, `cloudFiles.inferColumnTypes`).
- **Evolve schema** when new columns appear, and update the target table accordingly.
- This reduces manual work when the upstream system adds/removes fields.

---

### 3.5. File discovery modes (important for exams)

From the docs: [Microsoft Learn+1](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options?utm_source=chatgpt.com)

- **Directory listing mode** (default):
    - Periodically lists files under the input path.
    - Simpler, but less efficient for *huge* directories.
- **File notification mode**:
    - Uses storage-level **file events** (e.g., S3 bucket notifications) and queues.
    - Auto Loader subscribes to events and discovers only the new files since last time, avoiding full listings.
    - Configured via `cloudFiles.useNotifications` or newer managed file events options.

You may see exam questions about **what these two modes are and when to use notifications**.

---

### 3.6. Key options (very briefly)

From the options docs: [Microsoft Learn+1](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/options?utm_source=chatgpt.com)

- `cloudFiles.format` – file type (`csv`, `json`, `parquet`, `avro`, etc.).
- `cloudFiles.schemaLocation` – where schema & evolution metadata is stored.
- `cloudFiles.inferColumnTypes` – infer types instead of all strings for CSV.
- `cloudFiles.useNotifications` / `cloudFiles.useManagedFileEvents` – enable notification-based discovery.
- `cloudFiles.allowOverwrites` – whether to allow re-processing overwritten files.
- `cloudFiles.cleanSource` (newer runtimes) – optionally move/delete files after successful ingestion (archival/cost optimizations). [Databricks Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production?utm_source=chatgpt.com)

### Copy INTO Vs Auto-Loader

**COPY INTO**

- *Type*: Pure SQL command, batch.
- *Incrementality*: Idempotent; tracks loaded files and skips them. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/?utm_source=chatgpt.com)
- *Best for*: Simple, periodic loads; moderate file counts; easy jobs, dbt integration.
- *Pros*: Easy to understand, no streaming; simple scheduling; great for one-off bulk + incremental.
- *Cons*: For very large numbers of small files and near real-time needs, directory listing overhead becomes an issue.

**Auto Loader**

- *Type*: Structured Streaming source (`cloudFiles`).
- *Incrementality*: Uses checkpoints + file discovery (listing or notifications). [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/?utm_source=chatgpt.com)
- *Best for*: High-volume, continuous or frequent ingestion; large directories; complex schema drift.
- *Pros*: Highly scalable, schema inference/evolution, supports real-time/near real-time pipelines; integrates tightly with Delta Live Tables. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema?utm_source=chatgpt.com)
- *Cons*: Slightly more complex; you need to manage streaming semantics and checkpointing (unless DLT handles it).

## Multi-Hop Architecture

In Databricks, **Multi-Hop Architecture** is the pattern of building pipelines as **multiple sequential table layers (“hops”)** where each hop **progressively improves data quality/structure**. Databricks calls this the **Medallion Architecture (Bronze → Silver → Gold)** and explicitly notes it’s also referred to as **“multi-hop.”** [Databricks+1](https://www.databricks.com/glossary/medallion-architecture?utm_source=chatgpt.com)

- **Bronze:** raw/ingested as-is
- **Silver:** cleaned/conformed (dedupe, standardize, join/enrich)
- **Gold:** curated/aggregated for BI/analytics

### Why it’s used (what the exam looks for)

- **Separation of concerns:** ingestion ≠ cleaning ≠ business modeling.
- **Reusability:** many downstream “gold” products can reuse the same “silver”.
- **Reliability & lineage:** easier debugging, replay, and governance because each hop is a durable table.

### Mini multi-hop example

**Python (Lakeflow Spark Declarative Pipelines)**

Bronze uses streaming ingestion (e.g., Auto Loader / `spark.readStream`). [Databricks Documentation](https://docs.databricks.com/aws/en/ldp/streaming-tables)

Expectations apply data quality rules on records in these pipeline datasets. [Databricks Documentation](https://docs.databricks.com/aws/en/ldp/expectations)

```python
from pyspark import pipelines as dp

# BRONZE: ingest raw JSON files incrementally
@dp.table
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.inferColumnTypes", "true")
          .load("/Volumes/path/to/orders")
    )

# SILVER: clean + enforce data quality
@dp.table
@dp.expect("valid_order_id", "order_id IS NOT NULL")
@dp.expect("non_negative_amount", "amount >= 0")
def orders_silver():
    df = spark.readStream.table("orders_bronze")
    return (
        df.dropDuplicates(["order_id"])
          .select("order_id", "customer_id", "amount", "order_ts")
    )

# GOLD: business aggregation (example)
@dp.table
def daily_revenue_gold():
    df = spark.readStream.table("orders_silver")
    return df.groupByExpr("date(order_ts) as order_date").sum("amount")

```

**SQL (same idea)**

Databricks shows expectations as `CONSTRAINT ... EXPECT (...)` inside a streaming table definition. [Databricks Documentation+1](https://docs.databricks.com/aws/en/ldp/expectations)

```sql
CREATE OR REFRESH STREAMING TABLE orders_bronze
AS SELECT * FROM STREAM read_files("/volumes/path/to/orders", format => "json");

CREATE OR REFRESH STREAMING TABLE orders_silver (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL),
  CONSTRAINT non_negative_amount EXPECT (amount >= 0)
)
AS SELECT DISTINCT order_id, customer_id, amount, order_ts
FROM STREAM(orders_bronze);

CREATE OR REFRESH LIVE TABLE daily_revenue_gold
AS SELECT date(order_ts) AS order_date, sum(amount) AS revenue
FROM orders_silver
GROUP BY date(order_ts);

```

### Questions:

**Exam-style quick checks (with answers)**

1. Which layer is typically **raw and minimally transformed**? → **Bronze**. [Databricks+1](https://www.databricks.com/glossary/medallion-architecture?utm_source=chatgpt.com)
2. Where do you put **business aggregates for BI**? → **Gold**. [Databricks+1](https://www.databricks.com/glossary/medallion-architecture?utm_source=chatgpt.com)
3. What are **expectations** used for? → **Data quality constraints** during pipeline processing. [Databricks Documentation](https://docs.databricks.com/aws/en/ldp/expectations)
4. Streaming tables generally process each input row once; if you change the query, what happens to existing rows? → They **don’t recompute unless you do a full refresh**. [Databricks Documentation](https://docs.databricks.com/aws/en/ldp/streaming-tables)
5. Why multi-hop? → **Separation of concerns + reusability + easier debugging/lineage** across layers. [Databricks+1](https://www.databricks.com/glossary/medallion-architecture?utm_source=chatgpt.com)
