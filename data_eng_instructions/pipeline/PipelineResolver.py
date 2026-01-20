from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.manufacturing_factory.DefectPipeline import DefectPipeline
from data_eng_instructions.pipeline.manufacturing_factory.DowntimeReasonPipeline import DowntimeReasonPipeline
from data_eng_instructions.pipeline.manufacturing_factory.LineFactoryPipeline import LineFactoryPipeline
from data_eng_instructions.pipeline.manufacturing_factory.MachineStatePipeline import MachineStatePipeline
from data_eng_instructions.pipeline.manufacturing_factory.ManufacturingFactoryPipeline import \
    ManufacturingFactoryPipeline
from data_eng_instructions.pipeline.manufacturing_factory.OperatorPipeline import OperatorPipeline
from data_eng_instructions.pipeline.manufacturing_factory.OrderPipeline import OrderPipeline
from data_eng_instructions.pipeline.manufacturing_factory.ProductPipeline import ProductPipeline
from data_eng_instructions.pipeline.manufacturing_factory.ShiftPipeline import ShiftPipeline
from data_eng_instructions.pipeline.manufacturing_factory.WorkOrderStatusPipeline import WorkOrderStatusPipeline


class PipelineResolver:

    @staticmethod
    def resolve(pipeline_name: str, session: SparkSession) -> Pipeline:
        match pipeline_name:
            case "defect":
                return DefectPipeline(session)
            case "downtime_reason":
                return DowntimeReasonPipeline(session)
            case "line_factory":
                return LineFactoryPipeline(session)
            case "machine_state":
                return MachineStatePipeline(session)
            case "manufacturing_factory":
                return ManufacturingFactoryPipeline(session)
            case "operator":
                return OperatorPipeline(session)
            case "product":
                return ProductPipeline(session)
            case "order":
                return OrderPipeline(session)
            case "shift":
                return ShiftPipeline(session)
            case "work_order_status":
                return WorkOrderStatusPipeline(session)
            case _:
                raise ValueError(f"Unknown pipeline: {pipeline_name}")