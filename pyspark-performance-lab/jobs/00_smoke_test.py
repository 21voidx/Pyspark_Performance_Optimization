from common import build_spark, print_selected_conf
from pyspark.sql import functions as F

spark = build_spark("00-smoke-test")

print(f"SPARK_VERSION|{spark.version}")
print(f"MASTER|{spark.sparkContext.master}")
print(f"DEFAULT_PARALLELISM|{spark.sparkContext.defaultParallelism}")
print_selected_conf(
    spark,
    [
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.shuffle.partitions",
        "spark.sql.autoBroadcastJoinThreshold",
    ],
)

n = 5_000_000
result = spark.range(0, n).repartition(16).groupBy((F.col("id") % 10).alias("bucket")).count()
result.orderBy("bucket").show(10, truncate=False)

assert result.count() == 10
print("SMOKE_TEST|PASS")
spark.stop()
