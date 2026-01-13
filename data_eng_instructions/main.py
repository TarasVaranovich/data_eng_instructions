from pyspark.sql import SparkSession

# Create Spark session (local mode)
spark = SparkSession.builder \
    .appName("CSV SQL Example") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("people.csv")

# Show DataFrame
df.show()

# Stop Spark
spark.stop()