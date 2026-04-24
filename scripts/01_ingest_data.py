
from pyspark.sql import SparkSession
spark = SparkSession.
.appName("taxi_datapipeline")
.getOrCreate()
