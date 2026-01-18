from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from data_eng_instructions.utils.path_utility import project_root


class ManufacturingFactoryReader:
    def __init__(self, session: SparkSession, file_definition: FileDefinition):
        self.session = session
        self.file_definition = file_definition

    def read(self) -> DataFrame:
        session: SparkSession = self.session
        schema: StructType = self.file_definition.schema()
        file: str = self.file_definition.file()
        return session.read \
            .option("header", True) \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(f"{project_root()}{file}")
