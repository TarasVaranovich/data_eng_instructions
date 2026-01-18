from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from pyspark.sql.types import StructType, StructField, StringType

class ManufacturingFactoryDefinition(FileDefinition):

    def __init__(self):
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("factory_id", StringType(), True),
            StructField("line_id", StringType(), True),
            StructField("shift", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("planned_qty", StringType(), True),
            StructField("produced_qty", StringType(), True),
            StructField("scrap_qty", StringType(), True),
            StructField("defects_count", StringType(), True),
            StructField("cycle_time_s", StringType(), True),
            StructField("oee", StringType(), True),
            StructField("availability", StringType(), True),
            StructField("performance", StringType(), True),
            StructField("quality", StringType(), True),
            StructField("machine_state", StringType(), True),
            StructField("downtime_reason", StringType(), True),
            StructField("maintenance_type", StringType(), True),
            StructField("maintenance_due_date", StringType(), True),
            StructField("vibration_mm_s", StringType(), True),
            StructField("temperature_c", StringType(), True),
            StructField("pressure_bar", StringType(), True),
            StructField("operator_id", StringType(), True),
            StructField("workorder_status", StringType(), True)
        ])
        self._file = "manufacturing_factory_dataset.csv"
        self._schema = schema
        super().__init__(schema, "manufacturing_factory_dataset.csv")

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file