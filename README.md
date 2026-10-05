# Spark vs Ray: NYC Taxi Data Cleaning Benchmark

DA3408 Assignment 3.

I wrote the same data cleaning pipeline twice, once in PySpark and once in Ray Data. I ran each one on its own 2-worker Docker cluster against the NYC Yellow Taxi trip data, then compared the two on stage-by-stage time and resource usage.

## Folder structure

```
DA3408_Assignment3/
├── README.md
├── spark_clean.py            # pipeline in PySpark
├── ray_clean.py              # same pipeline in Ray Data
├── ray-docker-compose.yml    # 1 head + 2 workers for the Ray cluster
├── .gitignore                # keeps the big parquet files and outputs out of git
├── data/
│   └── taxi_zone_lookup.csv  # zone lookup table used in the join
│                             # (data/trips/ and data/output/ exist locally but are gitignored)
├── results/
│   ├── spark_metrics.json    # per-stage timings + row counts from the Spark run
│   ├── ray_metrics.json      # same for the Ray run
│   ├── spark_resources.txt   # docker stats log taken during the Spark run
│   └── ray_resources.txt     # docker stats log taken during the Ray run
└── Screenshots/
    ├── 01_spark_master_2_workers.png
    ├── 02_ray_dashboard_2_workers.png
    └── Screenshot From 2026-10-05 ... .png   # the mid-run captures (listed below)
```

The Spark cluster itself comes from a separate `docker-spark-cluster` repo (Spark 3.3.4, one master and two workers). I only edited its `docker-compose.yml` so the volumes point at my run folder, so it isn't copied in here.

## Dataset

NYC TLC Yellow Taxi trip records in parquet, **23 files**:

- 2023: months 2 to 12 (11 files)
- 2024: months 1 to 12 (12 files)

Plus `taxi_zone_lookup.csv` for the zone names. The raw data comes to **76,413,180 rows** before cleaning.

Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## What the pipeline does

Both scripts do the same five stages and time each one:

1. **Ingestion**: read all the parquet files and the zone CSV, count the rows.
2. **Cleansing**: keep the columns I need, drop nulls, drop duplicates, convert the pickup and dropoff columns to timestamps.
3. **Join**: join trips to the zone table twice, once for the pickup zone and once for the dropoff zone. Spark uses a normal join. Ray Data has no real join operator for this, so I load the small zone table as a pandas DataFrame and merge it inside `map_batches`.
4. **UDF**: compute `avg_speed_mph` (distance divided by trip time in hours) with a custom Python function. In Spark this is a Python UDF, in Ray it is a pandas batch function.
5. **Export**: write the result out as parquet.

Each script takes `--trips`, `--zones`, `--out` and `--metrics`, prints the metrics at the end and also saves them as JSON.

## How I ran it

Everything below is run from `~/spark-vs-ray-run`.

### 1. Folders and data

```bash
mkdir -p ~/spark-vs-ray-run/data/trips
mkdir -p ~/spark-vs-ray-run/data/output
mkdir -p ~/spark-vs-ray-run/screenshots
mkdir -p ~/spark-vs-ray-run/results
cd ~/spark-vs-ray-run
```

Put `spark_clean.py`, `ray_clean.py` and `ray-docker-compose.yml` in this folder.

Download the 23 parquet files and the zone lookup:

```bash
cd data/trips
for m in 02 03 04 05 06 07 08 09 10 11 12; do
  curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$m.parquet
done
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet
done
cd ..
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
cd ..
du -sh data/
```

### 2. Point the Spark cluster at my folder

In the `docker-spark-cluster` folder, edit `docker-compose.yml` and change the volumes of `spark-master`, `spark-worker-a` and `spark-worker-b`:

```yaml
volumes:
  - /home/rishitha/spark-vs-ray-run:/opt/spark-apps
  - /home/rishitha/spark-vs-ray-run/data:/opt/spark-data
```

(Swap in your own path.)

### 3. Start Spark

```bash
cd ~/docker-spark-cluster
docker compose up -d
docker ps
```

`docker ps` should show `spark-master`, `spark-worker-a`, `spark-worker-b` and `demo-database`. Then open `http://localhost:9090/` and check that 2 workers are alive. That is screenshot `01_spark_master_2_workers.png`.

### 4. Start Ray

```bash
cd ~/spark-vs-ray-run
docker compose -f ray-docker-compose.yml up -d
docker ps
```

You should see `ray-head`, `ray-worker-a` and `ray-worker-b`. Open `http://localhost:8265/` and check the cluster view shows both workers. That is screenshot `02_ray_dashboard_2_workers.png`.

### 5. Spark run

In a second terminal, start logging resource usage and leave it going for the whole job:

```bash
cd ~/spark-vs-ray-run
while true; do
  echo "$(date -u +%H:%M:%S) ---"
  docker stats --no-stream spark-master spark-worker-a spark-worker-b
  sleep 2
done | tee results/spark_resources.txt
```

In the main terminal, submit the job:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/spark_clean.py \
  --trips "/opt/spark-data/trips/*.parquet" \
  --zones /opt/spark-data/taxi_zone_lookup.csv \
  --out /opt/spark-data/output/spark_result.parquet \
  --metrics /opt/spark-data/output/spark_metrics.json
```

While it runs, open `http://localhost:4040/` (the live Spark application UI) and take screenshots with a stage still active. Once it finishes, `Ctrl+C` the stats terminal and copy out the metrics:

```bash
docker cp spark-master:/opt/spark-data/output/spark_metrics.json results/spark_metrics.json
cat results/spark_metrics.json
```

### 6. Ray run

Again, a second terminal for the resource log:

```bash
cd ~/spark-vs-ray-run
while true; do
  echo "$(date -u +%H:%M:%S) ---"
  docker stats --no-stream ray-head ray-worker-a ray-worker-b
  sleep 2
done | tee results/ray_resources.txt
```

Then run the job:

```bash
docker exec ray-head python /ray-apps/ray_clean.py \
  --trips /ray-data/trips \
  --zones /ray-data/taxi_zone_lookup.csv \
  --out /ray-data/output/ray_result.parquet \
  --metrics /ray-data/output/ray_metrics.json
```

Keep `http://localhost:8265/` open while it runs and screenshot it while tasks are actually active. When it finishes, `Ctrl+C` the stats terminal and copy out the metrics:

```bash
docker cp ray-head:/ray-data/output/ray_metrics.json results/ray_metrics.json
cat results/ray_metrics.json
```

### 7. Compare the two

```bash
python3 -c "
import json
s = json.load(open('results/spark_metrics.json'))
r = json.load(open('results/ray_metrics.json'))
stages = [
    ('ingestion_seconds', 'ingestion'),
    ('cleansing_seconds', 'cleaning'),
    ('join_seconds', 'join'),
    ('udf_seconds', 'udf'),
    ('export_seconds', 'export'),
    ('total_seconds', 'total'),
]
print(f'{\"stage\":12} {\"spark(s)\":>10} {\"ray(s)\":>10} {\"ray speedup\":>12}')
for key, label in stages:
    sv, rv = s[key], r[key]
    print(f'{label:12} {sv:10.3f} {rv:10.3f} {sv/rv:11.2f}x')
print()
for key in ['row_count_raw','row_count_after_cleaning','row_count_after_join','row_count_after_udf']:
    match = 'match' if s[key]==r[key] else 'MISMATCH'
    print(f'{key}: spark={s[key]} ray={r[key]} ({match})')
" | tee results/comparison.txt
```

## Results

Numbers come straight from `results/spark_metrics.json` and `results/ray_metrics.json`.

| Stage | Spark (s) | Ray (s) | Ray speedup |
|---|---:|---:|---:|
| Ingestion | 21.33 | 18.74 | 1.14x |
| Cleansing | 385.17 | 278.44 | 1.38x |
| Join | 341.03 | 241.86 | 1.41x |
| Python UDF | 315.43 | 263.61 | 1.20x |
| Export | 714.77 | 491.54 | 1.45x |
| **Total** | **1781.31** | **1294.19** | **1.38x** |

Row counts match exactly in both frameworks at every stage:

| Stage | Rows |
|---|---:|
| Raw | 76,413,180 |
| After cleaning | 76,413,173 |
| After join | 76,413,173 |
| After UDF | 76,413,173 |

Ray finished the whole pipeline about 1.38x faster than Spark on this setup.

## Screenshots

| File | What it shows |
|---|---|
| `01_spark_master_2_workers.png` | Spark Master UI (port 9090) with 2 workers alive |
| `02_ray_dashboard_2_workers.png` | Ray Dashboard (port 8265) with the cluster up, before the job |
| `Screenshot From 2026-10-05 08-26-49.png` | Spark application UI (port 4040), job 5 running |
| `Screenshot From 2026-10-05 08-26-58.png` | Spark application UI, active and completed jobs list |
| `Screenshot From 2026-10-05 08-32-37.png` | Spark Master UI with the SparkClean app running on both workers |
| `Screenshot From 2026-10-05 08-33-38.png` | Spark application UI later in the run, event timeline with more jobs finished |
| `Screenshot From 2026-10-05 09-12-31.png` | Ray Dashboard jobs tab, `ray_clean.py` in RUNNING state |
| `Screenshot From 2026-10-05 10-03-50.png` | Ray Dashboard overview during the run, 3 nodes active and 3.0/3.0 CPU in use |

## Notes

- `data/trips/` and `data/output/` are in `.gitignore` because of the size, so the parquet files and job outputs are not in the repo. Use the download commands above to get the data back.
- Resource usage was logged with `docker stats` every 2 seconds, saved in `results/*_resources.txt`.
