from abc import ABC
from typing import Generic, TypeVar

from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from data_eng_instructions.filedefinition.FileType import FileType

T = TypeVar('T')

class EntityReader(Generic[T], ABC):

    def __init__(self, session: SparkSession, file_definition: FileDefinition):
        self._session = session
        self._file_definition = file_definition

    """
    Current read implied read from size-restricted batch CSV file.
    Size validation has to be done on previous ETL stage.
    """

    def read_batch(self) -> DataFrame:
        file_type: str = self._file_definition.get_file_type()
        match file_type:
            case FileType.CSV:
                return self.read_csv()
            case FileType.PARQUET:
                return self.read_parquet()
            case _:
                raise ValueError(f"Unsupported file type: {file_type}")

    """
    Current read implied read from large storage, for example,
    CSV-files exported into delta-table.
    Query will be like: SELECT * FROM delta_table WHERE timestamp_column BETWEEN from_ts AND to_ts
    """

    def read_range(self, from_ts: T, to_ts: T) -> DataFrame:
        pass

    def read_csv(self) -> DataFrame:
        session: SparkSession = self._session
        schema: StructType = self._file_definition.get_schema()
        file: str = self._file_definition.get_file()
        return session.read \
            .option("header", True) \
            .option("delimiter", ",") \
            .schema(schema) \
            .csv(file)

    def read_parquet(self) -> DataFrame:
        session: SparkSession = self._session
        schema: StructType = self._file_definition.get_schema()
        file: str = self._file_definition.get_file()
        return (session
                .read
                .schema(schema)
                .parquet(file))

    def read_from_storage(self) -> DataFrame:
        file_type: FileType = self._file_definition.get_file_type()
        match file_type:
            case FileType.CSV:
                return self.read_csv()
            case FileType.PARQUET:
                return self.read_parquet()
            case _:
                raise NotImplementedError(f"No reader of type: {file_type}")