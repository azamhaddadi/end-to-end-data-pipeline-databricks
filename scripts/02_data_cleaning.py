from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count  
import pandas as pd

spark = SparkSession.builder \
.appName("taxi_validation") \
.getOrCreate()




raw_path = "data/raw/"
output_path = "data/output/"


df_kpi_2025 = spark.read.option("header","true").option("inferSchema", "true").csv(f"{output_path}kpi_2025.csv")
clean_trips_2025 =  spark.read.option("header","true").option("inferSchema", "true").csv(f"{output_path}clean_trips_2025_sample.csv")

total_record_count = clean_trips_2025.count()
total_amount_null_count  = clean_trips_2025.filter(col("total_amount").isNull()).count()
trip_distance_null_count  = clean_trips_2025.filter(col("trip_distance").isNull()).count()
total_amount_less_equal_zero_count  = clean_trips_2025.filter(col("total_amount") <= 0).count()
trip_distance_less_equal_zero_count = clean_trips_2025.filter(col("trip_distance") <= 0).count()
duplicate_count = total_record_count -  clean_trips_2025.drop_duplicates().count()


validate_report = {
    "total_record_count" : total_record_count, 
    "total_amount_null_count" : total_amount_null_count, 
    "trip_distance_null_count" : trip_distance_null_count,
    "total_amount_less_equal_zero_count" : total_amount_less_equal_zero_count,
    "trip_distance_less_equal_zero_count" : trip_distance_less_equal_zero_count,
    "duplicate_count" : duplicate_count     
}

print(validate_report)

pd.DataFrame([validate_report]).to_csv(f"{output_path}validate_report_2025.csv", index= False)

spark.stop()