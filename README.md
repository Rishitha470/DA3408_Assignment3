# Spark vs Ray

For DA3408 A3. Same data cleaning pipeline written twice, once in Spark and
once in Ray, run on a two worker cluster each, then compared on speed and
resource use.

## Files

- `spark_clean.py` — loads the NYC taxi trip data and the zone lookup
  table, drops nulls and duplicates, joins trips to pickup/dropoff zones,
  runs a python UDF for average speed, writes parquet out. Every stage is
  timed and dumped to a JSON file at the end.
- `ray_clean.py` — the exact same five stages, written against Ray Data
  instead. Ray doesn't have a real join operator so that step is done by
  broadcasting the zone table and merging it inside `map_batches`.

Both scripts take the same kind of arguments, `--trips`, `--zones`,
`--out`, `--metrics`, and print a metrics summary at the end on top of
writing it to disk.

## Data

NYC TLC Yellow Taxi trip data, a few months of parquet files to get to
roughly 2GB, plus the zone lookup table. Official page:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Files currently published at:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Check the TLC page directly in case that's moved by the time you read
this, they've changed hosting before.

## Running it

See the separate commands doc for the full step by step, cluster setup,
exact spark-submit / docker exec calls, what to screenshot and where
results land. Screenshots, metrics JSON, and the comparison table go in
this repo too once I've actually run both benchmarks, just not committed
yet since none of that should be faked ahead of time.
