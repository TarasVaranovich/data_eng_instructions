from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.operatorsrooster.OperatorsRoosterDefinition import OperatorsRoosterDefinition
from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER_CSV
from data_eng_instructions.utils.path_utility import project_root


class OperatorsRoosterDefinitionSource(OperatorsRoosterDefinition):

    def __init__(self):
        file: str = f"{project_root()}{"/resources/"}operators_roster.csv"
        schema = OPERATORS_ROOSTER_CSV
        file_type = FileType.CSV
        self._file = file
        self._schema = schema
        self._file_type = file_type
        super().__init__(schema, file, file_type)

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file

    def get_file_type(self) -> FileType:
        return self._file_type
