from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.shift.ShiftDefinition import ShiftDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class ShiftReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: ShiftDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
