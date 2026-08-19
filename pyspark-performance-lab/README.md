# PySpark Performance Lab — Local Docker

A small, reproducible Spark standalone lab for learning **how and why** common PySpark performance problems happen before moving the same mental model to GCP.

## Goal

Use real NYC taxi data to diagnose and test:

- Spark job / stage / task behavior
- shuffle
- broadcast vs distributed join
- natural data skew
- Parquet file layout
- small-file problem
- `repartition()` vs `coalesce()`
- AQE and physical plans

The main baseline intentionally uses normal Spark behavior. **AQE and automatic broadcast are not disabled.** Controlled experiments may force a strategy only to isolate a concept.

---

## 1. Architecture

```text
Official NYC TLC Parquet + Taxi Zone CSV
                 |
                 v
          host bind mount
           ./data/raw
                 |
                 v
+------------------------------------------------+
| Docker Compose                                 |
|                                                |
| Spark Master ---- Worker 1                     |
|       |       \-- Worker 2                     |
|       |                                        |
|       +---- spark-submit / PySpark driver      |
|                                                |
| Spark event logs ------> History Server        |
+------------------------------------------------+
                 |
        +--------+---------+
        v                  v
 ./output/baseline   ./output/optimized
```

This lab uses a shared Docker bind mount instead of HDFS/GCS. That is deliberate: the learning target is Spark execution, not Hadoop administration. When migrated to GCP, local paths map naturally to GCS and the Docker Spark cluster maps to Managed Service for Apache Spark.

---

## 2. Source data

Source: **NYC Taxi & Limousine Commission (TLC) Trip Record Data**, published by the City of New York.

Main dataset:

```text
Yellow Taxi Trip Records — monthly PARQUET
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
```

The download script uses TLC's official file endpoint pattern:

```text
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

Dimension lookup:

```text
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Why this dataset:

- real public operational data;
- millions of rows per multi-month run;
- native Parquet;
- small taxi-zone dimension is useful for join experiments;
- pickup-location distribution can be inspected for natural skew;
- easy to scale from 1 month to 12 months without changing code.

The project defaults to **2025** to keep one consistent annual schema. TLC notes that 2025+ Yellow Taxi data includes `cbd_congestion_fee`; this lab does not depend on that field.

---

# Manual setup on a Linux home server

## 3. Prerequisites

Check the host first:

```bash
docker --version
docker compose version
curl --version
free -h
df -h .
```

Recommended starting point:

```text
Host RAM:   8 GB minimum for this 2-worker lab
Free disk:  10+ GB for a small multi-month run
CPU:        4 logical cores or more
```

If your server is smaller, reduce worker resources in `.env` and start with one month.

---

## 4. Create project configuration

From the project root:

```bash
cp .env.example .env
```

Review it:

```bash
nano .env
```

Default lab allocation:

```text
Worker 1: 2 cores, 2 GB
Worker 2: 2 cores, 2 GB
```

Do not allocate all host memory to Spark.

---

## 5. Prepare writable directories

Spark applications and the History Server share the event-log directory.

```bash
mkdir -p spark-events output/baseline output/optimized benchmark data/prepared
chmod 1777 spark-events output output/baseline output/optimized benchmark data/prepared
```

These are generated-data directories only. `1777` is used here to avoid UID mismatches between the host user and the non-root Spark container. Keep source/config directories at normal permissions.

---

## 6. Pull the pinned Spark image

```bash
docker compose pull
```

This repo pins:

```text
apache/spark:4.2.0-python3
```

Check:

```bash
docker image ls | grep spark
```

---

## 7. Start Spark standalone

```bash
docker compose up -d
```

Check containers:

```bash
docker compose ps
```

Expected services:

```text
spark-master
spark-worker-1
spark-worker-2
spark-history
```

Inspect master logs:

```bash
docker compose logs --tail=100 spark-master
```

Inspect workers:

```bash
docker compose logs --tail=100 spark-worker-1
docker compose logs --tail=100 spark-worker-2
```

---

## 8. Open the UIs securely

By default the Docker ports bind only to `127.0.0.1` on the server.

From your laptop, create an SSH tunnel:

```bash
ssh \
  -L 19080:127.0.0.1:19080 \
  -L 19081:127.0.0.1:19081 \
  -L 19082:127.0.0.1:19082 \
  -L 18080:127.0.0.1:18080 \
  -L 14040:127.0.0.1:14040 \
  YOUR_USER@YOUR_HOME_SERVER
```

Then on the laptop:

```text
Spark Master UI:     http://localhost:19080
Worker 1 UI:         http://localhost:19081
Worker 2 UI:         http://localhost:19082
History Server:      http://localhost:18080
Live Application UI: http://localhost:14040
```

On the Master UI verify **2 ALIVE workers** before continuing.

If you deliberately bind the UI to a LAN/Tailscale address, change `UI_BIND_ADDRESS` in `.env`; do not expose an unsecured Spark standalone UI directly to the public internet.

---

# Data setup

## 9. First download: one month only

For the first smoke run:

```bash
./scripts/download_data.sh 2025 1 1
```

This downloads:

```text
data/raw/yellow/yellow_tripdata_2025-01.parquet
data/lookup/taxi_zone_lookup.csv
```

Check:

```bash
ls -lh data/raw/yellow/
ls -lh data/lookup/
du -sh data/
```

Do not start with the full year. Prove the environment first.

---

# First Spark run

## 10. Run the cluster smoke test

Manual command:

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/work-dir/project/jobs/00_smoke_test.py
```

Or the convenience wrapper:

```bash
./scripts/submit.sh jobs/00_smoke_test.py
```

Expected final line:

```text
SMOKE_TEST|PASS
```

While it is running, inspect the live Application UI. After it finishes, open the History Server.

What to learn here:

```text
Application -> Job -> Stage -> Task -> Executor
```

---

## 11. Inspect the real dataset

```bash
./scripts/submit.sh jobs/01_inspect_data.py
```

This prints:

- Spark version and relevant optimizer config;
- schema;
- number of input files;
- input Spark partitions;
- total rows;
- date range;
- top `PULocationID` values for skew inspection;
- taxi-zone dimension sample.

Do not optimize anything yet.

---

# Scale the dataset gradually

## 12. Development size: three months

```bash
./scripts/download_data.sh 2025 1 3
```

Rerun inspection:

```bash
./scripts/submit.sh jobs/01_inspect_data.py
```

Use this size while learning Spark UI and fixing code.

## 13. Benchmark size: full year

Only after the 3-month version is comfortable:

```bash
./scripts/download_data.sh 2025 1 12
```

The script skips files that already exist.

The useful benchmark size is determined by your server, not by an arbitrary row target. You want jobs long enough to expose stages, shuffle, file scans, and task imbalance without causing the host to swap continuously.

---

# Main workload

## 14. Baseline

Business workload:

```text
Yellow Taxi trips
   -> minimal cleaning
   -> join Taxi Zone dimension
   -> aggregate daily pickup-zone performance
   -> Parquet
```

Run:

```bash
./scripts/submit.sh jobs/02_baseline.py
```

Output:

```text
output/baseline/daily_zone_performance/
```

Important: this is a **normal baseline**. AQE and automatic broadcast are still enabled.

Inspect the printed physical plan and History Server. Specifically record:

```text
join type
Exchange nodes
Shuffle Read
Shuffle Write
stage duration
task count
max task duration
spill
```

---

## 15. Optimized candidate

```bash
./scripts/submit.sh jobs/03_optimized.py
```

This candidate makes two explicit choices:

1. explicitly broadcasts the tiny zone dimension;
2. writes the curated result partitioned by month.

It is a **hypothesis**, not a guaranteed improvement. If the baseline already uses `BroadcastHashJoin`, explicit broadcast may provide no runtime benefit. That is a useful result: Spark already optimized the join.

Output:

```text
output/optimized/daily_zone_performance/
```

Compare:

```bash
cat benchmark/results.csv
find output/baseline -name '*.parquet' | wc -l
find output/optimized -name '*.parquet' | wc -l
```

Do not put an improvement percentage in the README/CV until repeated runs support it.

---

# Isolated experiments

## 16. Shuffle

```bash
./scripts/submit.sh experiments/01_shuffle.py
```

Find `Exchange` in the plan. In History Server inspect the stage boundary and Shuffle Read/Write.

Question to answer:

> Why did `groupBy()` require redistribution of rows?

---

## 17. Broadcast join

Normal Spark optimizer:

```bash
./scripts/submit.sh experiments/02_broadcast_join.py --variant auto
```

Controlled distributed/shuffle join for learning:

```bash
./scripts/submit.sh experiments/02_broadcast_join.py --variant shuffle
```

Explicit broadcast:

```bash
./scripts/submit.sh experiments/02_broadcast_join.py --variant broadcast
```

Compare physical plans:

```text
BroadcastHashJoin
vs
SortMergeJoin / other distributed strategy
```

The forced `shuffle` variant is a controlled experiment, **not the normal baseline**.

---

## 18. Natural skew report

```bash
./scripts/submit.sh experiments/03_skew_report.py
```

The script reports the largest pickup key and a `MAX_TO_AVG_RATIO`.

Then use Spark UI to decide whether the imbalance actually becomes an execution problem. A skewed key distribution alone does not automatically justify salting.

---

## 19. Small-file problem

Create intentionally poor file layout:

```bash
./scripts/submit.sh experiments/04_small_files.py --variant small --small-partitions 200
```

Create compact layout:

```bash
./scripts/submit.sh experiments/04_small_files.py --variant compact --compact-partitions 8
```

Count files:

```bash
find data/prepared/small_files_small -name '*.parquet' | wc -l
find data/prepared/small_files_compact -name '*.parquet' | wc -l
```

Read both:

```bash
./scripts/submit.sh experiments/05_read_small_files.py --variant small
./scripts/submit.sh experiments/05_read_small_files.py --variant compact
```

Compare scan task count and elapsed time.

---

## 20. `repartition()` vs `coalesce()`

```bash
./scripts/submit.sh experiments/06_repartition_vs_coalesce.py --method repartition --partitions 8
./scripts/submit.sh experiments/06_repartition_vs_coalesce.py --method coalesce --partitions 8
```

Use the physical plan and Stage metrics. Do not conclude that `coalesce()` is universally faster; it avoids a full redistribution in cases where redistribution is unnecessary, but `repartition()` can produce better balance when data must be redistributed.

---

# Benchmark workflow

See:

```text
docs/BENCHMARK_GUIDE.md
```

Simple runtime/file-count results are appended to:

```text
benchmark/results.csv
```

For portfolio-quality evidence, manually capture from History Server:

```text
Shuffle Read
Shuffle Write
Spill
Task count
Max task duration
Physical join
```

Run important before/after comparisons multiple times on the same input and resources.

---

# Useful Docker commands

Status:

```bash
docker compose ps
```

Logs:

```bash
docker compose logs -f spark-master
```

Stop without deleting local data:

```bash
docker compose down
```

Start again:

```bash
docker compose up -d
```

Clear generated test artifacts but keep raw TLC files:

```bash
./scripts/reset_generated.sh
```

---

# Local -> GCP mental mapping

| Local lab | GCP equivalent |
|---|---|
| `data/raw/` bind mount | Cloud Storage (`gs://.../raw/`) |
| Docker Spark standalone | Managed Service for Apache Spark |
| local Parquet output | GCS / BigQuery |
| Spark event logs / UI | managed Spark observability / Cloud Logging |
| same PySpark DataFrame code | PySpark job on managed compute |

Hive/HDFS/Kafka are intentionally excluded from v1. Add them only when a learning goal requires them.
