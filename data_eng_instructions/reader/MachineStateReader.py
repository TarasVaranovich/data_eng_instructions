from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

from data_eng_instructions.filedefinition.machinestate.MachineStateDefinition import MachineStateDefinition
from data_eng_instructions.filedefinition.order.OrderDefinition import OrderDefinition
from data_eng_instructions.filedefinition.product.ProductDefinition import ProductDefinition
from data_eng_instructions.reader.EntityReader import EntityReader


class MachineStateReader(EntityReader[DateType]):

    def __init__(
            self,
            session: SparkSession,
            file_definition: MachineStateDefinition
    ):
        self.session = session
        self.file_definition = file_definition
        super().__init__(session, file_definition)
