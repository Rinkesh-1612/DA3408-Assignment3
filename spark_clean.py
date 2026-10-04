"""Assignment 3: Spark pipeline on a cluster with 2 workers.
To run: spark-submit spark_clean.py "data/raw/*.parquet"
"""
import glob, shutil, sys, time
from functools import reduce
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType

files = sys.argv[1] if len(sys.argv) > 1 else "data/raw/*.parquet"
out, tmp = "output/spark_features.parquet", "output/_spark_tmp"
spark = (SparkSession.builder.master("spark://spark-master:7077").appName("taxi-spark")
         .config("spark.driver.memory", "2g")
         .config("spark.executor.memory", "3g").config("spark.executor.cores", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.execution.pythonUDF.arrow.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
step = spark.sparkContext.setJobDescription  # Gives each job a name in the Spark UI.
start = time.time()

# 1. Ingestion. Column types are different between years. Thus cast each file first.
step("1. Ingestion")
types = {"tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp", "passenger_count": "int",
         "trip_distance": "double", "PULocationID": "int", "DOLocationID": "int",
         "fare_amount": "double", "tip_amount": "double", "total_amount": "double"}
cast = [F.col(c).cast(t).alias(c) for c, t in types.items()]
df = reduce(DataFrame.unionByName, [spark.read.parquet(f).select(cast) for f in sorted(glob.glob(files))])
rows_in = df.count()
print("rows loaded:", rows_in)

# 2. Cleansing.
df = df.dropna().dropDuplicates()
df = df.filter(F.col("tpep_pickup_datetime").between("2022-01-01", "2025-07-01")
               & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
               & (F.col("trip_distance") > 0) & (F.col("fare_amount") > 0))
df = (df.withColumn("pickup_time", F.date_format("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dropoff_time", F.date_format("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss")))

# 3. Transformation. Join the trips with the zone lookup (pickup and dropoff). Then apply the speed UDF.
step("3. Heavy join - load zone lookup")
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
for side, key in (("pickup", "PULocationID"), ("dropoff", "DOLocationID")):
    df = df.join(zones.select(F.col("LocationID").alias(key), F.col("Borough").alias(f"{side}_borough"),
                              F.col("Zone").alias(f"{side}_zone")), on=key, how="left")
df = df.withColumn("duration_sec", F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))


def avg_speed(distance, seconds):  # Custom Python UDF. Gives the speed in miles per hour.
    return distance / (seconds / 3600) if seconds and seconds > 0 else None


speed_udf = F.udf(avg_speed, DoubleType())

step("2+3. Cleansing + heavy join - write temp")
df.write.mode("overwrite").parquet(tmp)  # Write once. Thus the UDF timing measures only the speed calculation.
df = spark.read.parquet(tmp)

step("3. UDF timing - Python UDF")
t = time.time(); df.select(F.sum(speed_udf("trip_distance", "duration_sec"))).collect(); udf_s = time.time() - t
step("3. UDF timing - built-in expression")
t = time.time(); df.select(F.sum(F.col("trip_distance") / (F.col("duration_sec") / 3600))).collect(); native_s = time.time() - t
print(f"UDF_SECONDS python_udf={udf_s:.1f} native_expr={native_s:.1f}")

df = df.withColumn("avg_speed_mph", speed_udf("trip_distance", "duration_sec")).filter(F.col("avg_speed_mph").between(1, 80))
df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
df = df.join(df.groupBy("pickup_hour").agg(F.avg("avg_speed_mph").alias("avg_speed_per_hour")), on="pickup_hour")

# 4. Export.
step("3+4. Speed per hour + export to parquet")
df.write.mode("overwrite").parquet(out)
step("4. Count saved rows")
result = spark.read.parquet(out)
rows_out = result.count()
print(f"rows saved: {rows_out}  (removed {rows_in - rows_out})")
print(f"SPARK total time: {time.time() - start:.1f}s")
result.agg(F.round(F.sum("avg_speed_mph"), 2).alias("sum_speed"), F.round(F.sum("fare_amount"), 2).alias("sum_fare")).show()
spark.stop()
shutil.rmtree(tmp, ignore_errors=True)
