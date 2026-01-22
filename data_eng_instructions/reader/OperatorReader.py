from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

from data_eng_instructions.filedefinition.operator.OperatorDefinition import OperatorDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class OperatorReader(EntityReader[TimestampType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: OperatorDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
