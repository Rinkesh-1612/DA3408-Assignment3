# Assignment 3: short commands. The file docker-compose.yml defines the clusters.
# Run only one cluster at a time:
#   make spark-up  ->  make spark-run  ->  make spark-down
#   make ray-up    ->  make ray-run    ->  make ray-down
# Each run writes its output to logs/.
# Each run also records the CPU and memory of each container every 5 seconds (monitor.py).
# For a smaller run: make spark-run FILES="data/raw/yellow_tripdata_2024-*.parquet"

PY    := ../.venv/bin/python
FILES ?= data/raw/*.parquet

spark-up:
	docker compose --profile spark up -d

spark-run:
	@mkdir -p logs
	@$(PY) monitor.py logs/spark_resources.csv spark &
	docker compose exec -T spark-master /opt/spark/bin/spark-submit spark_clean.py "$(FILES)" 2>&1 | tee logs/spark_run.log

spark-down:
	docker compose --profile spark down

ray-up:
	docker compose --profile ray up -d

ray-run:
	@mkdir -p logs
	@$(PY) monitor.py logs/ray_resources.csv ray &
	docker compose exec -T ray-head python ray_clean.py "$(FILES)" 2>&1 | tee logs/ray_run.log

ray-down:
	docker compose --profile ray down

.PHONY: spark-up spark-run spark-down ray-up ray-run ray-down
