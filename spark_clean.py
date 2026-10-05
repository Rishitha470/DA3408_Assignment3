import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--trips", required=True)
    p.add_argument("--zones", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--metrics", required=True)
    p.add_argument("--master", default="spark://spark-master:7077")
    return p.parse_args()


def main():
    args = parse_args()
    metrics = {"framework": "spark"}
    t_start = time.perf_counter()

    spark = SparkSession.builder.appName("SparkClean").master(args.master).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.perf_counter()
    trips = spark.read.parquet(args.trips)
    zones = spark.read.option("header", True).option("inferSchema", True).csv(args.zones)
    row_count_raw = trips.count()
    metrics["ingestion_seconds"] = time.perf_counter() - t0
    metrics["row_count_raw"] = row_count_raw

    t0 = time.perf_counter()
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "total_amount"]
    cols = [c for c in cols if c in trips.columns]
    trips_clean = trips.select(*cols).dropna().dropDuplicates()
    trips_clean = trips_clean.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    trips_clean = trips_clean.withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    row_count_clean = trips_clean.count()
    metrics["cleansing_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_cleaning"] = row_count_clean

    t0 = time.perf_counter()
    pu = zones.selectExpr("LocationID as PULocationID", "Zone as PUZone")
    do = zones.selectExpr("LocationID as DOLocationID", "Zone as DOZone")
    joined = trips_clean.join(pu, "PULocationID").join(do, "DOLocationID")
    row_count_joined = joined.count()
    metrics["join_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_join"] = row_count_joined

    t0 = time.perf_counter()

    def avg_speed(distance, pickup, dropoff):
        if None in (distance, pickup, dropoff):
            return None
        hours = (dropoff - pickup).total_seconds() / 3600.0
        return float(distance) / hours if hours > 0 else None

    speed_udf = F.udf(avg_speed, DoubleType())
    with_speed = joined.withColumn("avg_speed_mph", speed_udf("trip_distance", "tpep_pickup_datetime", "tpep_dropoff_datetime"))
    udf_count = with_speed.count()
    metrics["udf_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_udf"] = udf_count

    t0 = time.perf_counter()
    with_speed.write.mode("overwrite").parquet(args.out)
    metrics["export_seconds"] = time.perf_counter() - t0
    metrics["total_seconds"] = time.perf_counter() - t_start

    with open(args.metrics, "w") as f:
        json.dump(metrics, f, indent=2)

    print(metrics)
    spark.stop()


if __name__ == "__main__":
    main()
