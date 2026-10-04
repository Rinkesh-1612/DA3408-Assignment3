# AI Disclosure

Rinkesh (DA24B017), DA3408 Assignment 3

I used Claude Code (Anthropic's Sonnet 5.5 and Opus 5.5 models) as an assistant for this assignment.
This file states what the AI did and what I did.

## What the AI helped with

| Part | AI help |
|---|---|
| `spark_clean.py`, `ray_clean.py` | Wrote the first drafts of both pipelines.|
| Ray redesign | My first Ray version stopped because it ran out of memory. The AI read the Ray logs, found the cause and suggested the hash-bucket design that is in the final code. The report explains this suggestion in Section 6, "Performance Tuning Note". |
| `docker-compose.yml` | Wrote the cluster and network configuration, with a comment on each setting. I read each setting until I could explain it. |
| `docker-compose.yml` | Verified my written networks and clusters settings if something is not getting broken inherently.|
| `Makefile`, `monitor.py` | Wrote the short run commands and the CPU and memory recorder(because i naively couldnt just keep on taking scrreenshots of htop eevry 5 seconds to monitor peak). |
|
| Figures | Made the three report figures from my resource logs (`logs/*_resources.csv`). |
| Report and README |helped in making the report concise and presentable  |


## What I did

- I decided on the main choices: the full dataset, Docker for the clusters, equal limits for both clusters, and simple code.
- I started the clusters and ran both final pipelines myself (`make spark-up`, `make spark-run`, `make ray-up`, `make ray-run`).
- I took all the screenshots of the Spark UI, the Ray dashboard and `htop` during these runs.
- I checked the results, the code and the report, and I recorded the video.
- I wrote the cluster and network configuration, with a comment on each setting.  |

## Performance numbers

The AI did not generate any performance numbers.
All times, CPU values, memory values and row counts in the report come from real runs on my machine.
The run logs are in `logs/`, and the screenshots are in `screenshots/`.
