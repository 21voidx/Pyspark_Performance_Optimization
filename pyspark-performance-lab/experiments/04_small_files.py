"""Controlled small-file lab using real TLC rows.

This intentionally creates bad files in variant=small so the failure mode is visible.
It is not presented as the project's baseline pipeline.
"""
import argparse
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import append_benchmark, build_spark, load_trips

parser = argparse.ArgumentParser()
parser.add_argument("--variant", choices=["small", "compact"], required=True)
parser.add_argument("--small-partitions", type=int, default=200)
parser.add_argument("--compact-partitions", type=int, default=8)
args = parser.parse_args()

spark = build_spark(f"exp-04-small-files-{args.variant}")
base = load_trips(spark).select(
    "tpep_pickup_datetime", "PULocationID", "DOLocationID", "trip_distance", "total_amount"
)

root = Path("/opt/spark/work-dir/project/data/prepared")
output = root / f"small_files_{args.variant}"
if output.exists():
    shutil.rmtree(output)

if args.variant == "small":
    to_write = base.repartition(args.small_partitions)
    note = f"Intentionally bad: repartition({args.small_partitions}) before write."
else:
    to_write = base.coalesce(args.compact_partitions)
    note = f"Compacted with coalesce({args.compact_partitions}); validate file sizes, not just file count."

start = time.perf_counter()
to_write.write.mode("overwrite").parquet(str(output))
elapsed = time.perf_counter() - start
append_benchmark("small_files", args.variant, elapsed, output, note)

print(f"BENCHMARK|experiment=small_files|variant={args.variant}|elapsed_sec={elapsed:.3f}")
print(f"OUTPUT|{output}")
print("NEXT|Compare file count/size, then run experiments/05_read_small_files.py on each output.")
spark.stop()
