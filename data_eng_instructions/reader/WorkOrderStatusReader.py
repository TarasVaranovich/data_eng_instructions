from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.workorderstatus.WorkOrderStatusDefinition import WorkOrderSatusDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class WorkOrderStatusReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: WorkOrderSatusDefinition,
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
