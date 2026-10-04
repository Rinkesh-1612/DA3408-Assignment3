"""Records the CPU and memory of each cluster container every 5 seconds during a job.

To run: python monitor.py <csv file> <spark|ray>
The script stops when the job (spark_clean.py or ray_clean.py) stops.
"""
import csv, os, subprocess, sys, time
import psutil

out, cluster = sys.argv[1], sys.argv[2]
job = f"{cluster}_clean.py"


def job_running():  # The host can also see the processes in the containers.
    for p in psutil.process_iter(["cmdline"]):
        cmd = " ".join(p.info["cmdline"] or [])
        if p.pid != os.getpid() and job in cmd:
            return True
    return False


def gigabytes(text):  # Changes "1.5GiB" or "512MiB" to GB.
    units = {"KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "kB": 1e3, "MB": 1e6, "GB": 1e9, "B": 1}
    for unit, size in units.items():
        if text.endswith(unit):
            return round(float(text[:-len(unit)]) * size / 1e9, 3)
    return 0.0


for _ in range(120):  # Wait until the job starts.
    if job_running():
        break
    time.sleep(1)

psutil.cpu_percent()  # The first call only starts the measurement.
with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["time", "container", "cpu_pct_of_one_core", "mem_gb", "machine_cpu_pct", "machine_mem_used_gb"])
    while job_running():
        stats = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}};{{.CPUPerc}};{{.MemUsage}}"],
                               capture_output=True, text=True).stdout.splitlines()
        vm, now = psutil.virtual_memory(), time.strftime("%Y-%m-%d %H:%M:%S")
        machine_cpu = psutil.cpu_percent()  # Average since the last sample.
        for line in stats:
            name, cpu, mem = line.split(";")
            if name.startswith(f"da-aiops-a3-{cluster}"):
                w.writerow([now, name.replace("da-aiops-a3-", ""), float(cpu.rstrip("%")), gigabytes(mem.split(" / ")[0]),
                            machine_cpu, round((vm.total - vm.available) / 1e9, 2)])
        f.flush()
        time.sleep(5)
