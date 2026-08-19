# Benchmark Guide

Record conclusions only after repeated runs on the same server, dataset, Spark image, and resource limits.

For each experiment, capture:

| Metric | Where to find it |
|---|---|
| Elapsed time | script output + `benchmark/results.csv` |
| Physical join | SQL tab / `explain("formatted")` |
| Shuffle Read | Spark UI / History Server, Stage details |
| Shuffle Write | Spark UI / History Server, Stage details |
| Spill | Spark UI / History Server, Stage details |
| Number of tasks | Stage details |
| Max task duration | Stage details |
| Input partitions | script output / scan stage |
| Output file count | `find <path> -name '*.parquet' | wc -l` |
| File sizes | `find <path> -name '*.parquet' -printf '%s\n'` or `du -ah` |

## Benchmark discipline

1. Use the same input files for before/after.
2. Keep AQE and automatic broadcast at their normal configured values for the main baseline.
3. Run each comparison at least 3 times if you plan to quote a runtime percentage in a CV.
4. Treat the first run separately because OS/filesystem caches can affect later runs.
5. A controlled experiment may force a bad strategy (`MERGE` hint, excessive repartitioning), but label it clearly; do not call it the normal baseline.
6. Do not claim an optimization worked if the physical plan or metrics did not materially change.
7. Prefer causal metrics (shuffle bytes, spill, file count, task skew) over runtime alone.
