from common import build_spark, load_trips, load_zones, print_selected_conf
from pyspark.sql import functions as F

spark = build_spark("01-inspect-nyc-tlc")
trips = load_trips(spark)
zones = load_zones(spark)

print("=== SPARK CONFIG ===")
print_selected_conf(
    spark,
    [
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.shuffle.partitions",
        "spark.sql.autoBroadcastJoinThreshold",
    ],
)

print("=== TRIP SCHEMA ===")
trips.printSchema()

print("=== INPUT FILES ===")
print(f"INPUT_FILE_COUNT|{len(trips.inputFiles())}")
print(f"INPUT_PARTITIONS|{trips.rdd.getNumPartitions()}")

print("=== COUNTS ===")
print(f"TRIP_ROWS|{trips.count()}")
print(f"ZONE_ROWS|{zones.count()}")

print("=== DATE RANGE ===")
trips.select(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
).show(truncate=False)

print("=== TOP PICKUP LOCATION IDS: inspect natural skew ===")
(
    trips.groupBy("PULocationID")
    .count()
    .orderBy(F.desc("count"))
    .show(25, truncate=False)
)

print("=== TAXI ZONE LOOKUP SAMPLE ===")
zones.orderBy("LocationID").show(10, truncate=False)

spark.stop()
