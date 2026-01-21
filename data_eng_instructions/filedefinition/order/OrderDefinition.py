from abc import ABC

from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from data_eng_instructions.filedefinition.FileType import FileType


class OrderDefinition(FileDefinition, ABC):

    def __init__(self, schema: StructType, file: str, file_type: FileType):
        file: str = file
        schema = schema
        self._file = file
        self._schema = schema
        self._file_type = file_type
        super().__init__(schema, file, file_type)