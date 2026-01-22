from typing import Any

from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import IntegerType

from data_eng_instructions.constant.stubs import DEFAULT_ID, SHOW_COUNT
from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.line_factory.dwh.LineFactoryDefinitionDWH import LineFactoryDefinitionDWH
from data_eng_instructions.filedefinition.machinestate.dwh.MachineStateDefinitionDWH import MachineStateDefinitionDWH
from data_eng_instructions.filedefinition.maintenancetype.dwh.MaintenanceTypeDefinitionDWH import \
    MaintenanceTypeDefinitionDWH
from data_eng_instructions.filedefinition.manufctoringfactory.source.ManufacturingFactoryDefinitionSource import \
    ManufacturingFactoryDefinitionSource
from data_eng_instructions.filedefinition.operator.dwh.OperatorDefinitionDWH import OperatorDefinitionDWH
from data_eng_instructions.filedefinition.order.dwh.OrderDefinitionDWH import OrderDefinitionDWH
from data_eng_instructions.filedefinition.product.dwh.ProductDefinitionDWH import ProductDefinitionDWH
from data_eng_instructions.filedefinition.shift.dwh.ShiftDefinitionDWH import ShiftDefinitionDWH
from data_eng_instructions.filedefinition.workorderstatus.dwh.WorkOrderStatusDefinitionDWH import \
    WorkOrderStatusDefinitionDWH
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.reader.LineFactoryReader import LineFactoryReader
from data_eng_instructions.reader.MachineStateReader import MachineStateReader
from data_eng_instructions.reader.MaintenanceTypeReader import MaintenanceTypeReader
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.reader.OperatorReader import OperatorReader
from data_eng_instructions.reader.OrderReader import OrderReader
from data_eng_instructions.reader.ProductReader import ProductReader
from data_eng_instructions.reader.ShiftReder import ShiftReader
from data_eng_instructions.reader.WorkOrderStatusReader import WorkOrderStatusReader
from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type, to_fact
from data_eng_instructions.utils.path_utility import storage_path
from data_eng_instructions.writer.ManufacturingFactoryFactWriter import ManufacturingFactoryFactWriter


class ManufacturingFactoryPipeline(Pipeline):

    def __init__(self, param: PipelineParam):
        self._param = param
        super().__init__(param)

    def run(self) -> Any:
        print("Running Manufacturing Factory pipeline.")

        file_type: FileType = self._param.get_result_type()
        spark: SparkSession = self._param.get_spark()

        print("Read products:")
        product_definition: ProductDefinitionDWH = ProductDefinitionDWH(file_type)
        product_reader: ProductReader = ProductReader(spark, product_definition)
        product_df: DataFrame = product_reader.read_from_storage()
        product_df.show(SHOW_COUNT)

        print("Read orders:")
        order_definition: OrderDefinitionDWH = OrderDefinitionDWH(file_type)
        order_reader: OrderReader = OrderReader(spark, order_definition)
        order_df: DataFrame = order_reader.read_from_storage()
        order_df.show(SHOW_COUNT)

        print("Read machine states:")
        ms_definition: MachineStateDefinitionDWH = MachineStateDefinitionDWH(file_type)
        ms_reader: MachineStateReader = MachineStateReader(spark, ms_definition)
        ms_df: DataFrame = ms_reader.read_from_storage()
        ms_df.show(SHOW_COUNT)

        print("Read work order statuses:")
        wos_definition: WorkOrderStatusDefinitionDWH = WorkOrderStatusDefinitionDWH(file_type)
        wos_reader: WorkOrderStatusReader = WorkOrderStatusReader(spark, wos_definition)
        wos_df: DataFrame = wos_reader.read_from_storage()
        wos_df.show(SHOW_COUNT)

        print("Read line factories:")
        lf_definition: LineFactoryDefinitionDWH = LineFactoryDefinitionDWH(file_type)
        lf_reader: LineFactoryReader = LineFactoryReader(spark, lf_definition)
        lf_df: DataFrame = lf_reader.read_from_storage()
        lf_df.show(SHOW_COUNT)

        print("Read shifts:")
        sh_definition: ShiftDefinitionDWH = ShiftDefinitionDWH(file_type)
        sh_reader: ShiftReader = ShiftReader(spark, sh_definition)
        sh_df: DataFrame = sh_reader.read_from_storage()
        sh_df.show(SHOW_COUNT)

        print("Read operators:")
        op_definition: OperatorDefinitionDWH = OperatorDefinitionDWH(file_type)
        op_reader: OperatorReader = OperatorReader(spark, op_definition)
        op_df: DataFrame = op_reader.read_from_storage()
        op_df.show(SHOW_COUNT)

        print("Read maintenance types:")
        mt_definition: MaintenanceTypeDefinitionDWH = MaintenanceTypeDefinitionDWH(file_type)
        mt_reader: MaintenanceTypeReader = MaintenanceTypeReader(spark, mt_definition)
        mt_df: DataFrame = mt_reader.read_from_storage()
        mt_df.show(SHOW_COUNT)

        print("Read manufacturing factories:")
        mf_definition: ManufacturingFactoryDefinitionSource = ManufacturingFactoryDefinitionSource()
        mf_df: DataFrame = (ManufacturingFactoryReader(spark, mf_definition)
                            .read_batch()).transform(csv_to_type)

        mf_df.show(SHOW_COUNT)

        print("Join products:")
        mf_df_pr = (
            mf_df.alias("mf")
            .join(
                product_df.alias("joined"),
                F.col("mf.product_id") == F.col("joined.product_natural_key"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df.columns if c != "product_id"],
                F.coalesce(F.col("joined.product_id"), F.lit(DEFAULT_ID)).alias("product_id")
            )
        )

        mf_df_pr.show(SHOW_COUNT)

        print("Join orders:")
        mf_df_prd_ord = (
            mf_df_pr.alias("mf")
            .join(
                order_df.alias("joined"),
                F.col("mf.order_id") == F.col("joined.order_natural_key"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_pr.columns if c != "order_id"],
                F.coalesce(F.col("joined.order_id"), F.lit(DEFAULT_ID)).alias("order_id")
            )
        )

        mf_df_prd_ord.show(SHOW_COUNT)

        print("Join machine states:")
        mf_df_prd_ord_ms = (
            mf_df_prd_ord.alias("mf")
            .join(
                ms_df.alias("joined"),
                F.col("mf.machine_state") == F.col("joined.machine_state"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_prd_ord.columns if c != "machine_state"],
                F.coalesce(F.col("joined.machine_state_id"), F.lit(DEFAULT_ID)).alias("machine_state_id")
            )
        )

        mf_df_prd_ord_ms.show(SHOW_COUNT)

        print("Join work order statuses:")
        mf_df_prd_ord_ms_ws = (
            mf_df_prd_ord_ms.alias("mf")
            .join(
                wos_df.alias("joined"),
                F.col("mf.workorder_status") == F.col("joined.work_order_status"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_prd_ord_ms.columns if c != "workorder_status"],
                F.coalesce(F.col("joined.work_order_status_id"), F.lit(DEFAULT_ID)).alias("work_order_status_id")
            )
        )
        mf_df_prd_ord_ms_ws.show(SHOW_COUNT)

        print("Join line factories:")
        mf_df_prd_ord_ms_ws_lf = (
            mf_df_prd_ord_ms_ws.alias("mf")
            .join(
                lf_df.alias("lf"),
                (F.col("mf.line_id") == F.col("lf.line_natural_key")) &
                (F.col("mf.factory_id") == F.col("lf.factory_natural_key")),
                how="left"
            )
            .withColumn(
                "line_factory_id",
                F.coalesce(F.col("lf.line_factory_id"), F.lit(DEFAULT_ID))
            )
            .drop(
                "line_natural_key",
                "factory_natural_key",
                "factory_id",
                "line_id"
            )
        )

        mf_df_prd_ord_ms_ws_lf.show(SHOW_COUNT)

        print("Join shifts:")
        mf_df_prd_ord_ms_ws_lf_sh = (
            mf_df_prd_ord_ms_ws_lf.alias("mf")
            .join(
                sh_df.alias("joined"),
                F.col("mf.shift") == F.col("joined.shift_name"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_prd_ord_ms_ws_lf.columns if c != "shift"],
                F.coalesce(F.col("joined.shift_id"), F.lit(DEFAULT_ID)).alias("shift_id")
            )
        )
        mf_df_prd_ord_ms_ws_lf_sh.show(SHOW_COUNT)

        print("Join operators:")
        mf_df_prd_ord_ms_ws_lf_sh_op = (
            mf_df_prd_ord_ms_ws_lf_sh.alias("mf")
            .join(
                op_df.alias("joined"),
                F.col("mf.operator_id") == F.col("joined.operator_natural_key"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_prd_ord_ms_ws_lf_sh.columns if c != "operator_id"],
                F.coalesce(F.col("joined.operator_id"), F.lit(DEFAULT_ID)).alias("operator_id")
            )
        )
        mf_df_prd_ord_ms_ws_lf_sh_op.show(SHOW_COUNT)

        print("Join maintenance types:")
        mf_df_prd_ord_ms_ws_lf_sh_op_mt = (
            mf_df_prd_ord_ms_ws_lf_sh_op.alias("mf")
            .join(
                mt_df.alias("joined"),
                F.col("mf.maintenance_type") == F.col("joined.maintenance_type"),
                "left"
            )
            .select(
                *[F.col(f"mf.{c}") for c in mf_df_prd_ord_ms_ws_lf_sh_op.columns if c != "maintenance_type"],
                F.coalesce(F.col("joined.maintenance_type_id"), F.lit(DEFAULT_ID)).alias("maintenance_type_id")
            )
        )
        mf_df_prd_ord_ms_ws_lf_sh_op_mt.show(SHOW_COUNT)

        print("Indexing - identifiers assignation:")
        mf_df_prd_ord_ms_ws_lf_sh_op_indexed: DataFrame = \
            (mf_df_prd_ord_ms_ws_lf_sh_op_mt
             .withColumn("operating_period_id", monotonically_increasing_id().cast(IntegerType())))

        print("Manufacturing factory indexed")
        mf_df_prd_ord_ms_ws_lf_sh_op_indexed.show(5)
        print("Result:")
        result_df: DataFrame = mf_df_prd_ord_ms_ws_lf_sh_op_indexed.transform(to_fact)
        result_df.show(5)
        writer: ManufacturingFactoryFactWriter = ManufacturingFactoryFactWriter(result_df, storage_path())
        self.write_to_storage(writer, "manufacturing_factory_fact")
