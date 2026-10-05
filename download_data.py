"""Assignment 3: downloads the NYC TLC yellow taxi data from the official TLC page.
To run: python download_data.py
Files already in data/ are not downloaded again.
"""
import os, re, urllib.request

PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
FIRST, LAST = "2022-01", "2025-06"
ZONES = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
headers = {"User-Agent": "Mozilla/5.0"}

# Find the links of the monthly Parquet files on the TLC page.
html = urllib.request.urlopen(urllib.request.Request(PAGE, headers=headers)).read().decode()
links = sorted(set(re.findall(r'https://[^"\s]+yellow_tripdata_\d{4}-\d{2}\.parquet', html)))
links = [u for u in links if FIRST <= re.search(r"\d{4}-\d{2}", u).group() <= LAST]
links.append(ZONES)

os.makedirs("data/raw", exist_ok=True)
for url in links:
    name = url.split("/")[-1]
    path = f"data/{name}" if name.endswith(".csv") else f"data/raw/{name}"
    if not os.path.exists(path):  # Download only the missing files.
        urllib.request.urlretrieve(url, path)
    print(name, round(os.path.getsize(path) / 1e6, 1), "MB")
print(len(links), "files are in data/")
