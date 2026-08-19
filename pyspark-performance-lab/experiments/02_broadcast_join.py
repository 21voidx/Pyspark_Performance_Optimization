"""Controlled join-strategy lab.

Variants:
  auto      : normal Spark optimizer (recommended baseline)
  shuffle   : MERGE hint to deliberately demonstrate a distributed shuffle join
  broadcast : explicit broadcast hint for the small zone dimension
"""
import argparse
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import build_spark, load_trips, load_zones

parser = argparse.ArgumentParser()
parser.add_argument("--variant", choices=["auto", "shuffle", "broadcast"], default="auto")
args = parser.parse_args()

spark = build_spark(f"exp-02-join-{args.variant}")
trips = load_trips(spark).select("PULocationID", "total_amount")
zones = load_zones(spark).select("LocationID", "Borough", "Zone")

if args.variant == "broadcast":
    right = broadcast(zones)
elif args.variant == "shuffle":
    right = zones.hint("merge")
    trips = trips.hint("merge")
else:
    right = zones

joined = trips.join(right, trips.PULocationID == right.LocationID, "left")
result = joined.groupBy("Borough").agg(
    F.count("*").alias("trip_count"),
    F.sum("total_amount").alias("total_revenue"),
)

result.explain(mode="formatted")
start = time.perf_counter()
rows = result.collect()
elapsed = time.perf_counter() - start
for row in rows:
    print(row)
print(f"BENCHMARK|experiment=join|variant={args.variant}|elapsed_sec={elapsed:.3f}")
print("CHECK_UI|Compare physical join type + Shuffle Read/Write. Do not call forced-shuffle a normal baseline.")
spark.stop()
