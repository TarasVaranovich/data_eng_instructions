from abc import ABC, abstractmethod

from pyspark.sql.types import StructType


class FileDefinition(ABC):

    def __init__(self, schema: StructType, file: str):
        self.schema = schema
        self.path = file

    @abstractmethod
    def schema(self) -> StructType:
        pass

    @abstractmethod
    def file(self) -> str:
        pass
