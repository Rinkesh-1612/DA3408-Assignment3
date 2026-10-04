# DA3408 Assignment 3: Spark vs Ray on NYC Taxi Data

Rinkesh (DA24B017)

**Video explanation (3 minutes):** [Google Drive link](https://drive.google.com/file/d/1CH5vNGYR5JpBetOh_673y0MJMtOFnIQP/view?usp=sharing)

This project has one data preprocessing pipeline in two frameworks: Apache Spark and Ray Data.
Each framework runs on a cluster with two workers in Docker.
The project compares the execution time, the CPU and memory usage, and the cost of a Python UDF.

## Contents

| Item | Description |
|---|---|
| `spark_clean.py` | The Spark pipeline. |
| `ray_clean.py` | The Ray Data pipeline. |
| `docker-compose.yml` | The cluster configuration and the network setup. |
| `Makefile` | Short commands to start the clusters, run the pipelines and stop the clusters. |
| `monitor.py` | Records the CPU and memory of each container every 5 seconds during a run. |
| `Assignment3_ETL.ipynb` | The first version of the pipeline (local Spark). It also downloads the data. |
| `logs/` | The output of each run and the CPU and memory records. |
| `screenshots/spark/`, `screenshots/ray/` | Screenshots of the Spark UI, the Ray dashboard and `htop`. |
| `report/report.pdf` | The benchmark report. |
| [Video](https://drive.google.com/file/d/1CH5vNGYR5JpBetOh_673y0MJMtOFnIQP/view?usp=sharing) | A 3-minute explanation of the project (Google Drive). |

The folders `data/` and `output/` are not in Git, because they are too large.

## Data

The data is the NYC TLC yellow taxi trip data, January 2022 to June 2025.
It has 42 monthly Parquet files (2.35 GB, 143,219,428 rows).
The pipeline also uses the taxi zone lookup table (265 zones).

To download the data:

1. Open `Assignment3_ETL.ipynb`.
2. Run the first code cell.
3. Make sure that the Parquet files are in `data/raw/`.
4. Make sure that the file `data/taxi_zone_lookup.csv` is available.

## Pipeline

The two pipelines do the same steps:

1. **Ingestion.** Read all monthly files. Cast all files to one schema.
2. **Cleansing.** Remove rows with null values. Remove exact duplicates. Remove trips with bad dates, zero or negative duration, distance or fare. Format the timestamps as text.
3. **Transformation.** Join the trips with the zone lookup (pickup and dropoff). Calculate the average speed with a custom Python UDF. Remove speeds that are not between 1 and 80 mph. Add the average speed for each hour of the day.
4. **Export.** Write the result as Parquet files into `output/`.

Ray does two steps in a different way.
Ray puts each row into one of 64 buckets on disk. A hash of the row selects the bucket.
Then Ray removes the duplicates in each bucket.
Ray also merges the small lookup tables into each batch.
Thus the full data fits in memory. The result is the same as the Spark result.

## Cluster configuration and network

| Cluster | Container | Role | Limit |
|---|---|---|---|
| Spark | `spark-master` | Master and driver | 2 CPU, 4 GB |
| Spark | `spark-worker-1`, `spark-worker-2` | Worker (2 cores, 3 GB each) | 2 CPU, 6 GB each |
| Ray | `ray-head` | Head and driver (0 CPUs for data work) | 2 CPU, 4 GB |
| Ray | `ray-worker-1`, `ray-worker-2` | Worker (2 CPUs each) | 2 CPU, 6 GB each |

All containers use one private Docker network, `a3net`.
The containers find each other by service name.
Only the web UIs are open, and only on localhost:

| Port | UI |
|---|---|
| 8082 | Spark master |
| 4040 | Spark application (only during a run) |
| 8265 | Ray dashboard |

Each container mounts the project folder at `/app`. Thus all nodes use the same data and output folders.

## How to run

You must have Docker with Docker Compose.
The Makefile uses the Python environment in `../.venv`. This environment must have `psutil` for `monitor.py`.

Run only one cluster at a time.

To run the Spark pipeline:

1. Start the cluster: `make spark-up`
2. Run the pipeline: `make spark-run`
3. Stop the cluster: `make spark-down`

To run the Ray pipeline:

1. Start the cluster: `make ray-up`
2. Run the pipeline: `make ray-run`
3. Stop the cluster: `make ray-down`

For a smaller test, use fewer files:
`make spark-run FILES="data/raw/yellow_tripdata_2024-*.parquet"`

Each run writes these files:

- `logs/spark_run.log` or `logs/ray_run.log`: the output of the script, with the row counts, the times and the checksums.
- `logs/spark_resources.csv` or `logs/ray_resources.csv`: the CPU and memory of each container every 5 seconds.

**Caution:** a new run overwrites the log files of the last run.

## Results

| | Spark | Ray |
|---|---|---|
| Rows saved | 127,324,977 | 127,324,977 |
| Sum of average speed | 1,446,035,777.14 | 1,446,035,777.14 |
| Execution time | 1018.0 s | 443.0 s |
| Python UDF / built-in expression | 73.1 s / 2.2 s | 42.4 s / 4.2 s |
| Cluster CPU, average (cores) | 4.2 | 4.3 |
| Cluster memory, peak (GB) | 10.1 | 11.6 |

The two pipelines give the same output.
Ray was approximately 2.3 times faster.
The Python UDF cost in Ray was approximately half of the cost in Spark.
Ray used more memory.
The report (`report/report.pdf`) gives the full analysis.

## AI usage

I used AI help for code drafts, the Docker configuration and the report figures.
All performance values come from real runs on this machine.
The logs and the screenshots show these runs.
