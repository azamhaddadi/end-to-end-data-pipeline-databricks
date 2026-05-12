from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, lag, sum, avg, round, date_format, format_number

spark = SparkSession.builder \
.appName("taxi_location_analysis") \
.getOrCreate()

gold_path = "data/Gold/"

silver_path = "data/Silver/data_cleaning_silver.csv"

raw_path = "data/raw/"

df_lookup = spark.read.option("header","true").option("inferSchema", "true").csv(f"{raw_path}taxi_zone_lookup.csv")



df_lookup_transformed = df_lookup.withColumnRenamed("LocationID", "PULocationID") \
.withColumnRenamed("Borough", "PUBorough") \
.withColumnRenamed("Zone", "PUZone")




df_silver  = spark.read.csv(silver_path, header= True, inferSchema = True)

df_silver_transformed = df_silver \
.withColumn("pickup_month", date_format(col("pickup_month"), "yyyy-MM"))

#df_silver.printSchema()
#df_lookup_transformed.printSchema()
df_analysis = df_silver_transformed.join(df_lookup_transformed, df_lookup_transformed.PULocationID == df_silver_transformed.PULocationID, "inner") \
.groupBy("PUBorough","trip_year","pickup_month") \
.agg(
    count("*").alias("total_trips"),
    round(sum(col("total_amount")),2).alias("total_revenue"),
    round(avg(col("trip_distance")),2).alias("avg_trip_distance")
) \
.orderBy("total_revenue", ascending = False) \
.select("PUBorough","trip_year","pickup_month","total_trips","total_revenue","avg_trip_distance") 


df_analysis.show(10, truncate=False)

df_analysis.toPandas().to_csv(f"{gold_path}location_analysis_2025_2026.csv", index=False)
#df_analysis.printSchema()


window_spec = Window.partitionBy("PUBorough").orderBy("trip_year")


df_growth_analysis = df_analysis \
.withColumn("lag_revenue", lag(col("total_revenue")).over(window_spec)) \
.withColumn("lag_trips", lag(col("total_trips")).over(window_spec)) \
.withColumn(
    "revenue_growth_pct",
    round(((col("total_revenue") - col("lag_revenue")) / col("lag_revenue")) * 100, 2)
) \
.withColumn(
    "trips_growth_pct",
    round(((col("total_trips") - col("lag_trips")) / col("lag_trips")) * 100, 2) ) \
.select("PUBorough","trip_year","pickup_month","revenue_growth_pct","trips_growth_pct")


df_growth_analysis = df_growth_analysis.filter(col("trip_year") == 2026)

df_growth_analysis.show(10, truncate=False)

df_growth_analysis.toPandas().to_csv(
    f"{gold_path}location_growth_2025_vs_2026.csv",
    index=False
)

print("Growth analysis saved to Gold successfully")