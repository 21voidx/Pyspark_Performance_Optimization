import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from common import append_benchmark, build_spark, load_trips, load_zones

spark = build_spark("03-optimized-daily-zone-performance")

output = Path("/opt/spark/work-dir/project/output/optimized/daily_zone_performance")
if output.exists():
    shutil.rmtree(output)

# Explicit projection/filtering keeps intent visible even when Catalyst can infer some pruning.
trips = (
    load_trips(spark)
    .select(
        "tpep_pickup_datetime",
        "PULocationID",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )
    .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("trip_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("PULocationID").isNotNull())
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("total_amount") >= 0)
)

zones = load_zones(spark).select("LocationID", "Borough", "Zone")

# Explicit broadcast is an optimization hypothesis. If the baseline already auto-broadcasts,
# this may produce little/no runtime gain — that is a valid benchmark result.
joined = trips.join(
    broadcast(zones),
    trips.PULocationID == zones.LocationID,
    "left",
)

result = joined.groupBy("trip_month", "trip_date", "Borough", "Zone").agg(
    F.count("*").alias("trip_count"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg("fare_amount").alias("avg_fare"),
    F.avg("tip_amount").alias("avg_tip"),
    F.avg("trip_distance").alias("avg_trip_distance"),
)

# Organize the curated result for month-level pruning in downstream reads.
# Repartitioning by the same key before partitionBy limits cross-file mixing by month.
result = result.repartition("trip_month")

print("=== OPTIMIZED PHYSICAL PLAN ===")
result.explain(mode="formatted")

start = time.perf_counter()
(
    result.write.mode("overwrite")
    .partitionBy("trip_month")
    .parquet(str(output))
)
elapsed = time.perf_counter() - start

append_benchmark(
    experiment="main_pipeline",
    variant="optimized",
    elapsed_sec=elapsed,
    output_path=output,
    notes="Explicit broadcast + month-partitioned output. Validate plan and metrics before claiming improvement.",
)

print(f"BENCHMARK|variant=optimized|elapsed_sec={elapsed:.3f}")
print(f"OUTPUT|{output}")
spark.stop()
