from pyspark.sql.classic.dataframe import DataFrame
from pyspark.sql.types import StructType

from data_eng_instructions.schema.dwh.LineFactory import LINE_FACTORY
from data_eng_instructions.writer.EntityWriter import EntityWriter


class LineFactoryWriter(EntityWriter):

    def __init__(self, data_frame: DataFrame, root_dir: str, mode: str = "errorifexists"):
        self._data_frame = data_frame
        self._root_dir = root_dir
        self._mode = mode
        super().__init__(data_frame, root_dir, mode)

    def schema(self) -> StructType:
        return LINE_FACTORY
