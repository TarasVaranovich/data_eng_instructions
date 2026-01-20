from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from data_eng_instructions.schema.file.OperatorsRooster import OPERATORS_ROOSTER_CSV


class OperatorsRoosterDefinition(FileDefinition):

    def __init__(self):
        schema = OPERATORS_ROOSTER_CSV
        self._file = "operators_roster.csv"
        self._schema = schema
        super().__init__(schema, "operators_roster.csv")

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file
