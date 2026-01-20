from pyspark.sql import SparkSession

from data_eng_instructions.pipeline.Pipeline import Pipeline
from data_eng_instructions.pipeline.PipelineResolver import PipelineResolver

spark = SparkSession.builder \
    .appName("Data Eng Instructions") \
    .master("local[*]") \
    .getOrCreate()

"""
    Migration orchestration:
    1 stage - downtime_reason, product, order, machine_state, 
        work_order_status, defect, line_factory, shift
    2 stage - operator, shift
    3 stage - manufacturing_factory
"""
pipeline_name: str = "shift"

pipeline: Pipeline = PipelineResolver.resolve(pipeline_name, spark)
pipeline.run()

spark.stop()