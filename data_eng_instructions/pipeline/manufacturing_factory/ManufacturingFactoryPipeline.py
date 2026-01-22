from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.constant.stubs import DEFAULT_ID
from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.line_factory.dwh.LineFactoryDefinitionDWH import LineFactoryDefinitionDWH
from data_eng_instructions.filedefinition.machinestate.dwh.MachineStateDefinitionDWH import MachineStateDefinitionDWH
from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.filedefinition.order.dwh.OrderDefinitionDWH import OrderDefinitionDWH
from data_eng_instructions.filedefinition.product.dwh.ProductDefinitionDWH import ProductDefinitionDWH
from data_eng_instructions.filedefinition.shift.dwh.ShiftDefinitionDWH import ShiftDefinitionDWH
from data_eng_instructions.filedefinition.workorderstatus.dwh.WorkOrderStatusDefinitionDWH import \
    WorkOrderStatusDefinitionDWH
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.LineFactoryReader import LineFactoryReader
from data_eng_instructions.reader.MachineStateReader import MachineStateReader
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.reader.OperatorReader import OperatorReader
from data_eng_instructions.reader.OrderReader import OrderReader
from data_eng_instructions.reader.ProductReader import ProductReader
from data_eng_instructions.reader.ShiftReder import ShiftReader
from data_eng_instructions.reader.WorkOrderStatusReader import WorkOrderStatusReader
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type
from data_eng_instructions.filedefinition.operator.dwh.OperatorDefinitionDWH import OperatorDefinitionDWH


class ManufacturingFactoryPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Running Defect Pipeline - Manufacturing Factory")

        file_type: FileType = self._param.get_result_type()
        spark: SparkSession = self._param.get_spark()

        print("Read orders:")
        order_definition: OrderDefinitionDWH = OrderDefinitionDWH(file_type)
        order_reader: OrderReader = OrderReader(spark, order_definition)
        order_df: DataFrame = order_reader.read_from_storage()
        order_df.show(5)

        print("Read products:")
        product_definition: ProductDefinitionDWH = ProductDefinitionDWH(file_type)
        product_reader: ProductReader = ProductReader(spark, product_definition)
        product_df: DataFrame = product_reader.read_from_storage()
        product_df.show(5)

        print("Read machine statuses:")
        ms_definition: MachineStateDefinitionDWH = MachineStateDefinitionDWH(file_type)
        ms_reader: MachineStateReader = MachineStateReader(spark, ms_definition)
        ms_df: DataFrame = ms_reader.read_from_storage()
        ms_df.show(5)

        print("Read work order statuses:")
        wos_definition: WorkOrderStatusDefinitionDWH = WorkOrderStatusDefinitionDWH(file_type)
        wos_reader: WorkOrderStatusReader = WorkOrderStatusReader(spark, wos_definition)
        wos_df: DataFrame = wos_reader.read_from_storage()
        wos_df.show(5)

        print("Read shifts:")
        sh_definition: ShiftDefinitionDWH = ShiftDefinitionDWH(file_type)
        sh_reader: ShiftReader = ShiftReader(spark, sh_definition)
        sh_df: DataFrame = sh_reader.read_from_storage()
        sh_df.show(5)

        print("Read line factories:")
        lf_definition: LineFactoryDefinitionDWH = LineFactoryDefinitionDWH(file_type)
        lf_reader: LineFactoryReader = LineFactoryReader(spark, lf_definition)
        lf_df: DataFrame = lf_reader.read_from_storage()
        lf_df.show(5)

        print("Read operators:")
        op_definition: OperatorDefinitionDWH = OperatorDefinitionDWH(file_type)
        op_reader: OperatorReader = OperatorReader(spark, op_definition)
        op_df: DataFrame = op_reader.read_from_storage()
        op_df.show(5)


        print("Read manufacturing factories:")
        mf_definition: ManufacturingFactoryDefinitionSource = ManufacturingFactoryDefinitionSource()
        mf_df: DataFrame = (ManufacturingFactoryReader(spark, mf_definition)
                            .read_batch()).transform(csv_to_type)

        mf_df_fact = (
            mf_df.alias("mf")
            .join(
                order_df.alias("od"),
                F.col("mf.order_id") == F.col("od.order_natural_key"),
                how="left"
            )
            .withColumn(
                "order_id",
                F.coalesce(F.col("od.order_id"), F.lit(DEFAULT_ID))
            )
            .drop("order_natural_key", "od.order_id")
        )
