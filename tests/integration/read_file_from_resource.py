from pyspark.sql import SparkSession

from data_eng_instructions.utils.local_path import project_root

def read_file_from_resource(file_name: str, spark: SparkSession, lines: int):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(f"{project_root()}{file_name}")
    df.show(lines)