from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition import ManufacturingFactoryDefinition
from data_eng_instructions.utils.path_utility import project_root


class ManufacturingFactoryReader:
    def __init__(self, session: SparkSession, file_definition: ManufacturingFactoryDefinition):
        self.session = session
        self.file_definition = file_definition

    def read(self) -> DataFrame:
        session: SparkSession = self.session
        schema: StructType = self.file_definition.get_schema()
        file: str = self.file_definition.get_file()
        return session.read \
            .option("header", True) \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(f"{project_root()}{"/resources/"}{file}")
