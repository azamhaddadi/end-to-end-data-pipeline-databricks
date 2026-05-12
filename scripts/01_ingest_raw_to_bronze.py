from pyspark.sql import SparkSession
from glob import glob

spark = SparkSession.builder \
    .appName("taxi_ingest_raw_to_bronze") \
    .getOrCreate()

raw_path = "data/raw/"

files_2025 = glob(f"{raw_path}yellow_tripdata_2025-*.parquet")
files_2026 = glob(f"{raw_path}yellow_tripdata_2026-*.parquet")
lookup_file = glob(f"{raw_path}taxi_zone_lookup.csv")[0]

print("2025 files:")
for file in files_2025:
    print(file)

print("2026 files:")
for file in files_2026:
    print(file)

print("Lookup file:")
print(lookup_file)

df_2025 = spark.read.parquet(*files_2025)
df_2026 = spark.read.parquet(*files_2026)

df_lookup = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(lookup_file)

print("df_2025 count:", df_2025.count())
print("df_2026 count:", df_2026.count())
print("df_lookup count:", df_lookup.count())

print("df_2025 schema")
df_2025.printSchema()

print("df_2026 schema")
df_2026.printSchema()

print("df_lookup schema")
df_lookup.printSchema()

spark.stop()