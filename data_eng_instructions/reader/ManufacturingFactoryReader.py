from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

from data_eng_instructions.filedefinition.manufctoringfactory.ManufactoringFactoryDefinition import \
    ManufacturingFactoryDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class ManufacturingFactoryReader(EntityReader[TimestampType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: ManufacturingFactoryDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
