from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
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
    def resolve(pipeline_name: str, param: PipelineParam) -> Pipeline:
        match pipeline_name:
            case "defect":
                return DefectPipeline(param)
            case "downtime_reason":
                return DowntimeReasonPipeline(param)
            case "line_factory":
                return LineFactoryPipeline(param)
            case "machine_state":
                return MachineStatePipeline(param)
            case "manufacturing_factory":
                return ManufacturingFactoryPipeline(param)
            case "operator":
                return OperatorPipeline(param)
            case "product":
                return ProductPipeline(param)
            case "order":
                return OrderPipeline(param)
            case "shift":
                return ShiftPipeline(param)
            case "work_order_status":
                return WorkOrderStatusPipeline(param)
            case _:
                raise ValueError(f"Unknown pipeline: {pipeline_name}")