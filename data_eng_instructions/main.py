from pyspark.sql import SparkSession

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.pipeline.PipelineResolver import PipelineResolver

spark = SparkSession.builder \
    .appName("Data Eng Instructions") \
    .master("local[*]") \
    .getOrCreate()

"""
    Pipelines orchestration:
    1 stage (could be executed in parallel) - downtime_reason, product, order, machine_state, 
        work_order_status, defect, line_factory, shift, team, maintenance_type
    2 stage - operator
    3 stage - manufacturing_factory
    4 stage - operating_period_defect, operating_period_downtime_reason
"""
#pipeline_name: str = "maintenance_type"
pipeline_name: str = "manufacturing_factory"
param: PipelineParam = PipelineParam(spark, FileType.PARQUET)
pipeline: Pipeline = PipelineResolver.resolve(pipeline_name, param)
pipeline.run()

spark.stop()
