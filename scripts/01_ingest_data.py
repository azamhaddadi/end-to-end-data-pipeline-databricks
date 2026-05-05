
from pyspark.sql import SparkSession 
from pyspark.sql.functions import lit,month,to_timestamp, date_format, col, sum,count,avg, round

spark = SparkSession.builder \
.appName("taxi_datapipeline") \
.getOrCreate()

#print("Spark Started")
raw_path = "data/raw/"

output_path = "data/output/"

df_jan_2025 = spark.read.parquet(f"{raw_path}yellow_tripdata_2025-01.parquet")
#df_jan_2025.printSchema()
#print(f"count Jan 2025 ",df_jan_2025.count())
df_feb_2025 = spark.read.parquet(f"{raw_path}yellow_tripdata_2025-02.parquet")
#print("Count feb 2025", df_feb_2025.count())

df_2025 = df_jan_2025.unionByName(df_feb_2025)
#print("count 2025 ", df_2025.count())
raw_count = df_2025.count()
print(f"Raw 2025 record count: {raw_count}")
#df_2025 = spark.read.parquet(f"{raw_path}yellow_tripdata_2025-*.parquet")

df_2025_clean = (df_2025 \
.select("tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID","total_amount","trip_distance") \
.withColumn("pickup_month",date_format("tpep_pickup_datetime","yyyy-MM")) \
.withColumn("dropoff_month",date_format("tpep_dropoff_datetime","yyyy-MM")) \
.filter(col("pickup_month").isin("2025-01","2025-02")) \
.filter((col("total_amount") > 0 ) & (col("trip_distance") > 0 ))
)

clean_count = df_2025_clean.count()
print(f"Clean 2025 record count: {clean_count}")
print(f"Removed records: {raw_count - clean_count}")

df_2025_clean.limit(50000).toPandas().to_csv(f"{output_path}clean_trips_2025_sample.csv", index=False)

#df_2025.groupBy("pickup_month").count().show()


df_2025_kpi = df_2025_clean\
.groupBy("pickup_month") \
.agg(count("*").alias("trip_count") ,
     round(sum(col("total_amount")), 2).cast("decimal(12,2)").alias("total_revenue"),
     round(avg(col("trip_distance")),2).alias("avg_trip_distance")
) \
.orderBy("pickup_month")
#df_2025_kpi.orderBy("pickup_month").limit(5).show()
df_2025_kpi.show()

df_2025_kpi.toPandas().to_csv(f"{output_path}kpi_2025.csv", index=False)
spark.stop()

                

 
