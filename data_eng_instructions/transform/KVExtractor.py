from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, monotonically_increasing_id
from pyspark.sql.types import StructType, IntegerType

"""
A transformer that extracts key-value pairs from a DataFrame
Transformer introduced for interactions with two column tables
key - name of column representing identifier generated
value - name of column containing natural value represented in source
"""


class KVExtractor(object):
    def __init__(
            self,
            schema_from: StructType,
            schema_to: StructType,
            key: str,
            value_from: str,
            value_to: str
    ):
        self.schema_from = schema_from
        self.schema_to = schema_to
        self.key = key
        self.value_from = value_from
        self.value_to = value_to

    def extract(self, df: DataFrame) -> DataFrame:
        assert df.schema == self.schema_from
        transformed = (df
                       .select(col(self.value_from).alias(self.value_to))
                       .distinct()
                       .filter(col(self.value_to).isNotNull() & (trim(col(self.value_to)) != ""))
                       .withColumn(self.key, monotonically_increasing_id().cast(IntegerType()))
                       .select(self.key, self.value_to)
                       )
        spark = df.sparkSession
        return spark.createDataFrame(
            transformed.rdd,
            self.schema_to
        )
