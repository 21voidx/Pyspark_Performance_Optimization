"""Measure natural key imbalance before deciding whether skew mitigation is justified."""
import sys
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import build_spark, load_trips

spark = build_spark("exp-03-skew-report")
trips = load_trips(spark).filter(F.col("PULocationID").isNotNull())

counts = trips.groupBy("PULocationID").count().cache()
stats = counts.agg(
    F.max("count").alias("max_key_rows"),
    F.avg("count").alias("avg_key_rows"),
    F.expr("percentile_approx(count, 0.5)").alias("median_key_rows"),
).first()

print(f"MAX_KEY_ROWS|{stats['max_key_rows']}")
print(f"AVG_KEY_ROWS|{stats['avg_key_rows']}")
print(f"MEDIAN_KEY_ROWS|{stats['median_key_rows']}")
if stats["avg_key_rows"]:
    print(f"MAX_TO_AVG_RATIO|{stats['max_key_rows'] / stats['avg_key_rows']:.2f}")

counts.orderBy(F.desc("count")).show(30, truncate=False)
print("NEXT|Only add salting/skew mitigation if Spark UI shows straggler tasks or skewed partitions.")
spark.stop()
