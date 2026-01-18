from abc import ABC, abstractmethod

from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import TimestampType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition


class EntityReader(ABC):

    def __init__(self, session: SparkSession, file_definition: FileDefinition):
        self._session = session
        self._file_definition = file_definition

    """
    Current read implied read from size-restricted batch CSV file.
    Size validation has to be done on previous ETL stage.
    """

    @abstractmethod
    def read_csv_batch(self) -> DataFrame:
        pass

    """
    Current read implied read from large storage, for example,
    CSV-files exported into delta-table.
    Query will be like: SELECT * FROM delta_table WHERE timestamp_column BETWEEN from_ts AND to_ts
    """

    @abstractmethod
    def read_range(self, from_ts: TimestampType, to_ts: TimestampType) -> DataFrame:
        pass
