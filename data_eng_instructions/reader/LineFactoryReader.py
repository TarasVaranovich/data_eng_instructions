from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.line_factory.LineFactoryDefinition import LineFactoryDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class LineFactoryReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: LineFactoryDefinition,
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
