from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.shift.ShiftDefinition import ShiftDefinition
from data_eng_instructions.schema.dwh.Order import ORDER
from data_eng_instructions.schema.dwh.Shift import SHIFT
from data_eng_instructions.utils.path_utility import storage_path


class ShiftDefinitionDWH(ShiftDefinition):

    def __init__(self, file_type: FileType):
        file: str = f"{storage_path()}/{file_type.value}/shift.{file_type.value}"
        schema = SHIFT
        self._file = file
        self._schema = schema
        super().__init__(schema, file, file_type)

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file

    def get_file_type(self) -> FileType:
        return self._file_type