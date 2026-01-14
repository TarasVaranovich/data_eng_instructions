from pyspark.sql import SparkSession


def make_csv_local() -> SparkSession:
    spark = SparkSession.builder \
        .appName("CSV session") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp") \
        .config("spark.hadoop.fs.AbstractFileSystem.local.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .getOrCreate()
    return spark