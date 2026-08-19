"""Observe a real wide transformation and its Exchange/shuffle metrics in Spark UI."""
from pyspark.sql import functions as F

import sys
sys.path.insert(0, "/opt/spark/work-dir/project/jobs")
from common import build_spark, load_trips

spark = build_spark("exp-01-shuffle")
trips = load_trips(spark)

result = (
    trips.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
    .groupBy("trip_date", "PULocationID")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
)

result.explain(mode="formatted")
print(f"RESULT_ROWS|{result.count()}")
print("CHECK_UI|SQL/Stages tabs -> Exchange, Shuffle Read, Shuffle Write")
spark.stop()
