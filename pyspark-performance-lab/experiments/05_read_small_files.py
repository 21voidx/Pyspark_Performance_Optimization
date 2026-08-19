import argparse
import sys
import time

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import build_spark

parser = argparse.ArgumentParser()
parser.add_argument("--variant", choices=["small", "compact"], required=True)
args = parser.parse_args()

spark = build_spark(f"exp-05-read-small-files-{args.variant}")
path = f"/opt/spark/work-dir/project/data/prepared/small_files_{args.variant}"

df = spark.read.parquet(path)
print(f"INPUT_PARTITIONS|{df.rdd.getNumPartitions()}")

start = time.perf_counter()
result = df.agg(
    F.count("*").alias("rows"),
    F.sum("total_amount").alias("revenue"),
).first()
elapsed = time.perf_counter() - start

print(f"ROWS|{result['rows']}")
print(f"BENCHMARK|experiment=read_files|variant={args.variant}|elapsed_sec={elapsed:.3f}")
print("CHECK_UI|Compare scan tasks, scheduler overhead, and elapsed time.")
spark.stop()
