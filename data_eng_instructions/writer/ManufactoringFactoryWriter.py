from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType

from data_eng_instructions.schema.file.ManufacturingFactory import MANUFACTURING_FACTORY_CSV
from data_eng_instructions.writer.EntityWriter import EntityWriter


class ManufacturingFactoryWriter(EntityWriter):

    def __init__(self, data_frame: DataFrame, dir: str, mode: str = "errorifexists"):
        self._data_frame = data_frame
        self._dir = dir
        self._mode = mode
        super().__init__(data_frame, dir, mode)

    def schema(self) -> StructType:
        return MANUFACTURING_FACTORY_CSV