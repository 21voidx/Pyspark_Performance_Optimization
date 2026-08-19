from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path("/opt/spark/work-dir/project")
RAW_YELLOW = str(PROJECT_ROOT / "data/raw/yellow/*.parquet")
ZONE_LOOKUP = str(PROJECT_ROOT / "data/lookup/taxi_zone_lookup.csv")
BENCHMARK_CSV = PROJECT_ROOT / "benchmark/results.csv"


def build_spark(app_name: str) -> SparkSession:
    """Create a SparkSession using cluster/default configuration from spark-defaults.conf."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_trips(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(RAW_YELLOW)


def load_zones(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(ZONE_LOOKUP)
    )


def clean_trips(df: DataFrame) -> DataFrame:
    """Minimal domain cleaning used by the main workload."""
    return (
        df.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("trip_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("PULocationID").isNotNull())
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("total_amount") >= 0)
    )


def count_parquet_files(path: str | Path) -> int:
    p = Path(path)
    if not p.exists():
        return 0
    return sum(1 for f in p.rglob("*.parquet") if f.is_file())


def append_benchmark(
    experiment: str,
    variant: str,
    elapsed_sec: float,
    output_path: str | Path,
    notes: str = "",
) -> None:
    BENCHMARK_CSV.parent.mkdir(parents=True, exist_ok=True)
    exists = BENCHMARK_CSV.exists()
    row = {
        "run_ts_utc": datetime.now(timezone.utc).isoformat(),
        "experiment": experiment,
        "variant": variant,
        "elapsed_sec": f"{elapsed_sec:.3f}",
        "output_files": count_parquet_files(output_path),
        "notes": notes,
    }
    with BENCHMARK_CSV.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=row.keys())
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def print_selected_conf(spark: SparkSession, keys: Iterable[str]) -> None:
    for key in keys:
        try:
            value = spark.conf.get(key)
        except Exception:
            value = "<not set>"
        print(f"CONF|{key}={value}")
