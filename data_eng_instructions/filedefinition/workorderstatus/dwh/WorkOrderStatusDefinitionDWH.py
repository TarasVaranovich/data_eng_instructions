from pyspark.sql.types import StructType

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.workorderstatus.WorkOrderStatusDefinition import WorkOrderSatusDefinition
from data_eng_instructions.schema.dwh.WorkOrderStatus import WORK_ORDER_STATUS
from data_eng_instructions.utils.path_utility import storage_path


class WorkOrderStatusDefinitionDWH(WorkOrderSatusDefinition):

    def __init__(self, file_type: FileType):
        file: str = f"{storage_path()}/{file_type.value}/work_order_status.{file_type.value}"
        schema = WORK_ORDER_STATUS
        self._file = file
        self._schema = schema
        super().__init__(schema, file, file_type)

    def get_schema(self) -> StructType:
        return self._schema

    def get_file(self) -> str:
        return self._file

    def get_file_type(self) -> FileType:
        return self._file_type
