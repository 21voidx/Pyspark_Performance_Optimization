import shutil
import time
from pathlib import Path

from pyspark.sql import functions as F

from common import append_benchmark, build_spark, clean_trips, load_trips, load_zones

spark = build_spark("02-baseline-daily-zone-performance")

output = Path("/opt/spark/work-dir/project/output/baseline/daily_zone_performance")
if output.exists():
    shutil.rmtree(output)

# Intentionally ordinary DataFrame code. We keep Spark's normal optimizers enabled.
trips = clean_trips(load_trips(spark))
zones = load_zones(spark)

joined = trips.join(
    zones,
    trips.PULocationID == zones.LocationID,
    "left",
)

result = joined.groupBy("trip_date", "Borough", "Zone").agg(
    F.count("*").alias("trip_count"),
    F.sum("total_amount").alias("total_revenue"),
    F.avg("fare_amount").alias("avg_fare"),
    F.avg("tip_amount").alias("avg_tip"),
    F.avg("trip_distance").alias("avg_trip_distance"),
)

print("=== BASELINE PHYSICAL PLAN ===")
result.explain(mode="formatted")

start = time.perf_counter()
result.write.mode("overwrite").parquet(str(output))
elapsed = time.perf_counter() - start

append_benchmark(
    experiment="main_pipeline",
    variant="baseline",
    elapsed_sec=elapsed,
    output_path=output,
    notes="Normal Spark defaults; AQE and automatic broadcast remain enabled.",
)

print(f"BENCHMARK|variant=baseline|elapsed_sec={elapsed:.3f}")
print(f"OUTPUT|{output}")
spark.stop()
