"""Assignment 3: Ray Data pipeline on a cluster with 2 workers.
To run: python ray_clean.py "data/raw/*.parquet"
The steps and the result are the same as in spark_clean.py.
Ray removes duplicates in hash buckets on disk. Identical rows always go into the same bucket.
Ray merges the small lookup tables into each batch. Thus the full data fits in memory.
"""
import glob, os, shutil, sys, time
import pandas as pd
import ray

files = sys.argv[1] if len(sys.argv) > 1 else "data/raw/*.parquet"
out, buckets, staged = (os.path.abspath(f"output/{d}") for d in ("ray_features.parquet", "_ray_buckets", "_ray_staged"))
types = {"tpep_pickup_datetime": "datetime64[us]", "tpep_dropoff_datetime": "datetime64[us]", "passenger_count": "float64",
         "trip_distance": "float64", "PULocationID": "int64", "DOLocationID": "int64",
         "fare_amount": "float64", "tip_amount": "float64", "total_amount": "float64"}
zones = pd.read_csv("data/taxi_zone_lookup.csv")[["LocationID", "Borough", "Zone"]]


def avg_speed(distance, seconds):  # Custom Python UDF. Gives the speed in miles per hour.
    return distance / (seconds / 3600) if seconds > 0 else None


def clean(b):  # Cleansing. Also gives each row a bucket number from a hash of the row.
    b = b.astype(types).dropna()
    b = b[(b.tpep_pickup_datetime >= "2022-01-01") & (b.tpep_pickup_datetime <= "2025-07-01")
          & (b.tpep_dropoff_datetime > b.tpep_pickup_datetime) & (b.trip_distance > 0) & (b.fare_amount > 0)]
    b["bucket"] = pd.util.hash_pandas_object(b[list(types)], index=False) % 64
    return b


def process_bucket(row):  # Removes duplicates, joins the zones and applies the UDF. Returns the speed sums for each hour.
    df = pd.read_parquet(f"{buckets}/bucket={row['bucket']}").drop_duplicates()
    df["pickup_time"] = df.tpep_pickup_datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
    df["dropoff_time"] = df.tpep_dropoff_datetime.dt.strftime("%Y-%m-%d %H:%M:%S")
    for side, key in (("pickup", "PULocationID"), ("dropoff", "DOLocationID")):
        df = df.merge(zones.rename(columns={"LocationID": key, "Borough": f"{side}_borough", "Zone": f"{side}_zone"}),
                      on=key, how="left")
    df["duration_sec"] = (df.tpep_dropoff_datetime - df.tpep_pickup_datetime).dt.total_seconds().astype("int64")
    df["avg_speed_mph"] = [avg_speed(d, s) for d, s in zip(df.trip_distance, df.duration_sec)]
    df = df[df.avg_speed_mph.between(1, 80)].copy()
    df["pickup_hour"] = df.tpep_pickup_datetime.dt.hour
    df.to_parquet(f"{staged}/part-{row['bucket']:03d}.parquet", index=False)
    h = df.groupby("pickup_hour").avg_speed_mph.agg(["sum", "count"])
    return {"hour": h.index.tolist(), "sum": h["sum"].tolist(), "count": h["count"].tolist()}


ray.init(address="auto")
print("worker nodes:", [(n["NodeManagerAddress"], n["Resources"]["CPU"]) for n in ray.nodes() if n["Alive"] and n["Resources"].get("CPU")])
for d in (out, buckets, staged):
    shutil.rmtree(d, ignore_errors=True)  # Ray adds to old files. Thus remove them first.
os.makedirs(staged)
start = time.time()
try:
    # 1. Ingestion.
    ds = ray.data.read_parquet([os.path.abspath(f) for f in sorted(glob.glob(files))], columns=list(types))
    rows_in = ds.count()
    print("rows loaded:", rows_in)

    # 2. Cleansing. Write the rows into 64 hash buckets on disk.
    ds.map_batches(clean, batch_format="pandas").write_parquet(buckets, partition_cols=["bucket"])

    # 3. Transformation. For each bucket: remove duplicates, join the zones, apply the UDF.
    ids = sorted(int(p.split("=")[-1]) for p in glob.glob(f"{buckets}/bucket=*"))
    tasks = ray.data.from_items([{"bucket": i} for i in ids], override_num_blocks=len(ids))
    totals = pd.concat(pd.DataFrame(r) for r in tasks.map(process_bucket, num_cpus=1).take_all()).groupby("hour").sum()
    speed_per_hour = (totals["sum"] / totals["count"]).to_dict()
    trips = ray.data.read_parquet(staged)

    # UDF overhead. Do the speed calculation as a Python UDF and as a pandas expression.
    # Read the same 2 columns as Spark.
    cols = ray.data.read_parquet(staged, columns=["trip_distance", "duration_sec"])
    udf = cols.map_batches(lambda b: pd.DataFrame({"s": [avg_speed(d, s) for d, s in zip(b.trip_distance, b.duration_sec)]}),
                           batch_format="pandas")
    native = cols.map_batches(lambda b: pd.DataFrame({"s": b.trip_distance / (b.duration_sec / 3600)}), batch_format="pandas")
    t = time.time(); udf = udf.materialize(); udf_s = time.time() - t
    t = time.time(); native = native.materialize(); native_s = time.time() - t
    print(f"UDF_SECONDS python_udf={udf_s:.1f} native_expr={native_s:.1f}")
    print("UDF_STATS python_udf:\n", udf.stats(), "\nUDF_STATS native_expr:\n", native.stats())

    # 4. Export.
    trips.map_batches(lambda b: b.assign(avg_speed_per_hour=b.pickup_hour.map(speed_per_hour)),
                      batch_format="pandas").write_parquet(out)
    result = ray.data.read_parquet(out)
    rows_out = result.count()
    print(f"rows saved: {rows_out}  (removed {rows_in - rows_out})")
    print(f"RAY total time: {time.time() - start:.1f}s")
    print("sum_speed", round(result.sum("avg_speed_mph"), 2), "sum_fare", round(result.sum("fare_amount"), 2))
finally:
    shutil.rmtree(buckets, ignore_errors=True)
    shutil.rmtree(staged, ignore_errors=True)
    ray.shutdown()
