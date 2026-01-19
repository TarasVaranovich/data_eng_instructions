from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileDefinition import FileDefinition
from data_eng_instructions.schema.ManufacturingFactory import MANUFACTURING_FACTORY_CSV


class ManufacturingFactoryDefinition(FileDefinition):

    def __init__(self):
        schema = MANUFACTURING_FACTORY_CSV
        self._file = "manufacturing_factory_dataset.csv"
        self._schema = schema
        super().__init__(schema, "manufacturing_factory_dataset.csv")

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file