from pyspark.sql import SparkSession
from spark_cities.main import main

if __name__ == "__main__":
  spark = SparkSession \
    .builder \
    .appName("Spark Cities") \
    .getOrCreate()
  # Rend SPARK SUBMIT moins verbeux
  spark.sparkContext.setLogLevel("WARN")
  main(spark)
