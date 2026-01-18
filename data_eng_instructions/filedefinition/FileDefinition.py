from abc import ABC, abstractmethod

from pyspark.sql.types import StructType


class FileDefinition(ABC):

    def __init__(self, schema: StructType, file: str):
        self._schema = schema
        self._path = file

    @abstractmethod
    def get_schema(self) -> StructType:
        pass

    @abstractmethod
    def get_file(self) -> str:
        pass
