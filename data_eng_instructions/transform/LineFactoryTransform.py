from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, monotonically_increasing_id
from pyspark.sql.types import IntegerType

from data_eng_instructions.schema.dwh.LineFactory import LINE_FACTORY
from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY


def extract_line_factory(dataframe: DataFrame) -> DataFrame:
    assert dataframe.schema == MANUFACTURING_FACTORY
    lf: DataFrame = (
        dataframe
        .select(
            col("line_id").alias("line_natural_key"),
            col("factory_id").alias("factory_natural_key")
        )
        .filter(
            col("line_natural_key").isNotNull() &
            col("factory_natural_key").isNotNull() &
            (trim(col("line_natural_key")) != "") &
            (trim(col("factory_natural_key")) != "")
        )
        .distinct()
        .withColumn("line_factory_id", monotonically_increasing_id().cast(IntegerType()))
        .select("line_factory_id", "line_natural_key", "factory_natural_key")
    )
    spark = dataframe.sparkSession
    return spark.createDataFrame(
        lf.rdd,
        LINE_FACTORY
    )
