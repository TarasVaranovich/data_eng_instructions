from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.operatorsrooster import OperatorsRoosterDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class OperatorsRoosterReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: OperatorsRoosterDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
