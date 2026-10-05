import argparse
import json
import time

import pandas as pd
import ray
import ray.data


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--trips", required=True)
    p.add_argument("--zones", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--metrics", required=True)
    return p.parse_args()


def clean_batch(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", "total_amount"]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].dropna().drop_duplicates()
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
    return df


def udf_batch(df: pd.DataFrame) -> pd.DataFrame:
    hours = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 3600.0
    speed = df["trip_distance"] / hours
    speed[(hours <= 0) | hours.isna()] = None
    df["avg_speed_mph"] = speed
    return df


def main():
    args = parse_args()
    metrics = {"framework": "ray"}
    t_start = time.perf_counter()

    ray.init(address="auto")
    ray.data.DataContext.get_current().min_parallelism = 4

    t0 = time.perf_counter()
    ds = ray.data.read_parquet(args.trips)
    zones_pd = pd.read_csv(args.zones)
    row_count_raw = ds.count()
    metrics["ingestion_seconds"] = time.perf_counter() - t0
    metrics["row_count_raw"] = row_count_raw

    t0 = time.perf_counter()
    ds = ds.map_batches(clean_batch, batch_format="pandas")
    row_count_clean = ds.count()
    metrics["cleansing_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_cleaning"] = row_count_clean

    t0 = time.perf_counter()
    pu = zones_pd.rename(columns={"LocationID": "PULocationID", "Zone": "PUZone"})[["PULocationID", "PUZone"]]
    do = zones_pd.rename(columns={"LocationID": "DOLocationID", "Zone": "DOZone"})[["DOLocationID", "DOZone"]]

    def join_batch(df: pd.DataFrame) -> pd.DataFrame:
        out = df.merge(pu, on="PULocationID", how="inner")
        out = out.merge(do, on="DOLocationID", how="inner")
        return out

    ds = ds.map_batches(join_batch, batch_format="pandas")
    row_count_joined = ds.count()
    metrics["join_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_join"] = row_count_joined

    t0 = time.perf_counter()
    ds = ds.map_batches(udf_batch, batch_format="pandas")
    udf_count = ds.count()
    metrics["udf_seconds"] = time.perf_counter() - t0
    metrics["row_count_after_udf"] = udf_count

    t0 = time.perf_counter()
    ds.write_parquet(args.out)
    metrics["export_seconds"] = time.perf_counter() - t0
    metrics["total_seconds"] = time.perf_counter() - t_start

    with open(args.metrics, "w") as f:
        json.dump(metrics, f, indent=2)

    print(metrics)
    ray.shutdown()


if __name__ == "__main__":
    main()
