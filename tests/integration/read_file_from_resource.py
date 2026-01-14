from pyspark.sql import SparkSession, DataFrame

from data_eng_instructions.utils.path_utility import project_root

def read_file_from_resource(spark: SparkSession, file_name: str) -> DataFrame:
    return spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(f"{project_root()}{file_name}")