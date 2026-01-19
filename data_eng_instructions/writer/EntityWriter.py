from abc import ABC, abstractmethod

from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType


class EntityWriter(ABC):

    def __init__(self, data_frame: DataFrame, dir: str, mode: str, ):
        self._data_frame = data_frame
        self._mode = mode
        self._dir = dir

    @property
    @abstractmethod
    def schema(self) -> StructType:
        pass

    def write_csv(self, file_name: str):
        assert self._data_frame.schema == self.schema()
        (self._data_frame
         .write
         .mode(self._mode)
         .option("header", "true")
         .csv(f"{dir}/{file_name}"))

    def write_parquet(self, file_name: str):
        assert self._data_frame.schema == self.schema()
        (self._data_frame
         .write
         .mode(self._mode)
         .parquet(f"{dir}/{file_name}"))
