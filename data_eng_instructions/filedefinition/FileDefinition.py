from abc import ABC, abstractmethod

from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileType import FileType


class FileDefinition(ABC):

    def __init__(self, schema: StructType, file: str, file_type: FileType):
        self._schema = schema
        self._path = file
        self._file_type = file_type

    @abstractmethod
    def get_schema(self) -> StructType:
        pass

    @abstractmethod
    def get_file(self) -> str:
        pass

    @abstractmethod
    def get_file_type(self) -> FileType:
        pass
