from itertools import count
from pyspark.sql.functions import count,sum, avg , round , format_number,date_format, col 
from pyspark.sql import SparkSession 

silver_path = "data/Silver/data_cleaning_silver.csv"
gold_path = "data/Gold/kpi_2025.csv"

spark = SparkSession.builder \
.appName("taxi_kpi_gold") \
.getOrCreate()

df_silver = spark.read.option("header","true").option("inferschema", "true").csv(silver_path)


df_kpi = df_silver \
.withColumn("pickup_month", date_format(col("pickup_month"), "yyyy-MM")) \
.groupBy("trip_year","pickup_month") \
.agg(
    format_number(count("*"), 0).alias("total_trips"),
format_number(sum("total_amount"),2).alias("total_revenue"),
format_number(round(avg("trip_distance"), 2),2).alias("avg_trip_distance") 
)\
.select("pickup_month","total_trips","total_revenue","avg_trip_distance")

df_kpi.limit(10).show()
df_kpi.toPandas().to_csv(gold_path, index=False)