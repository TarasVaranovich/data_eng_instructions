from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType, TimestampType

from data_eng_instructions.filedefinition import ManufacturingFactoryDefinition
from data_eng_instructions.utils.path_utility import project_root


class ManufacturingFactoryReader:
    def __init__(self, session: SparkSession, file_definition: ManufacturingFactoryDefinition):
        self.session = session
        self.file_definition = file_definition
    """
    Current read implied read from size-restricted batch CSV file.
    Size validation has to be done on previous ETL stage.
    """
    def read_csv_batch(self) -> DataFrame:
        session: SparkSession = self.session
        schema: StructType = self.file_definition.get_schema()
        file: str = self.file_definition.get_file()
        return session.read \
            .option("header", True) \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(f"{project_root()}{"/resources/"}{file}")
    """
    Current read implied read from large storage, for example,
    CSV-files exported into delta-table.
    Query will be like: SELECT * FROM delta_table WHERE timestamp_column BETWEEN from_ts AND to_ts
    """
    def read_range(self, from_ts: TimestampType, to_ts: TimestampType) -> DataFrame:
        pass