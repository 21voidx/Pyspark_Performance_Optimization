"""Compare physical plans for reducing partition count."""
import argparse
import sys
import time

sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import build_spark, load_trips

parser = argparse.ArgumentParser()
parser.add_argument("--method", choices=["repartition", "coalesce"], required=True)
parser.add_argument("--partitions", type=int, default=8)
args = parser.parse_args()

spark = build_spark(f"exp-06-{args.method}")
df = load_trips(spark).select("PULocationID", "total_amount")

if args.method == "repartition":
    candidate = df.repartition(args.partitions)
else:
    candidate = df.coalesce(args.partitions)

candidate.explain(mode="formatted")
print(f"TARGET_PARTITIONS|{candidate.rdd.getNumPartitions()}")
start = time.perf_counter()
rows = candidate.count()
elapsed = time.perf_counter() - start
print(f"ROWS|{rows}")
print(f"BENCHMARK|experiment=partition_count|variant={args.method}|elapsed_sec={elapsed:.3f}")
print("CHECK_UI|Look for Exchange/shuffle; do not infer that coalesce is always better.")
spark.stop()
