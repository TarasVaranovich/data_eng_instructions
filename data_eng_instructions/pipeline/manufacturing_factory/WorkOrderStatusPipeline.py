from typing import Any

from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.pipeline.Pipeline import Pipeline


class WorkOrderStatusPipeline(Pipeline):

    def __init__(self, spark: SparkSession):
        self._spark = spark
        super().__init__(spark)

    def run(self) -> Any:
        print("Running Work Order Status Pipeline - Dummy Implementation")
        self._spark.range(1).count()
