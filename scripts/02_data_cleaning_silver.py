from pyspark.sql import  SparkSession
from pyspark.sql.functions import lit, col, date_format,year
from glob import glob
from logging import basicConfig, getLogger, INFO

import traceback

spark = SparkSession.builder \
    .appName("taxi_data_cleaning_silver") \
    .getOrCreate()

raw_path = "data/raw/"

file_2025 = glob(f"{raw_path}yellow_tripdata_2025-*.parquet")
file_2026 = glob(f"{raw_path}yellow_tripdata_2026-*.parquet")
file_lookup = glob(f"{raw_path}taxi_zone_lookup.csv")

df_2025 = spark.read.parquet(*file_2025)
df_2026 = spark.read.parquet(*file_2026)
df_lookup = spark.read.option("header","true").option("inferSchema", "true").csv(file_lookup[0])

for file in file_2025:
    print(file)

for file in file_2026:
    print(file)

for file in file_lookup:
    print(file)



df_2025_clean= df_2025\
    .withColumn("trip_year", lit(2025))
    

df_2026_clean = df_2026 \
    .withColumn("trip_year", lit(2026))
  
    
df_2025_sample = df_2025_clean.limit(500000)
df_2026_sample = df_2026_clean.limit(500000)

df_union = df_2025_sample.unionByName(df_2026_sample) \
.select("tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","PULocationID","DOLocationID","fare_amount","total_amount","trip_year")


print(f"Count join befrore cleaning :",df_union.count())     


df_silver = df_union.filter(( col("total_amount") > 0) &  (col("trip_distance") > 0 )) \
                .filter(col("tpep_pickup_datetime").isNotNull()) \
    .filter(col("tpep_dropoff_datetime").isNotNull()) \
    .withColumn("pickup_month", date_format(col("tpep_pickup_datetime"), "yyyy-MM"))


print(f"Count join after cleaning :",df_silver.count())        


df_silver.show(10, truncate = False)

try:
   # df_silver = df_silver.dropDuplicates()

    df_silver = df_silver.filter(year(col("tpep_pickup_datetime")) == col("trip_year")) 

    #print("Silver count after duplicate removal:", df_silver.count())
    
    silver_path = "data/Silver/data_cleaning_silver.csv"
    df_silver.toPandas().to_csv(silver_path, index=False)

    
    print(f"Silver data saved successfully to: {silver_path}")

    df_silver.groupBy("pickup_month").count().show()
    
    
except Exception as e:
    print(f"ERROR TYPE: {type(e).__name__}")
    print(f"ERROR MESSAGE: {e}")
    traceback.print_exc()
#df_2025.printSchema()
#df_2026.printSchema()
#df_lookup.printSchema()
spark.stop()


