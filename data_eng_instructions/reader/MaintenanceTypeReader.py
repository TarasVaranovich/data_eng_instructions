from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.maintenancetype.MaintenanceTypeDefinition import MaintenanceTypeDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class MaintenanceTypeReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: MaintenanceTypeDefinition,
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
