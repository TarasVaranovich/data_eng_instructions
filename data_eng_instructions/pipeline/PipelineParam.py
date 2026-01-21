from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.filedefinition.FileType import FileType


class PipelineParam:
    def __init__(self, spark: SparkSession, result_type: FileType):
        self.__spark = spark
        self.__result_type = result_type

    def get_name(self) -> str:
        return self.__name

    def get_spark(self) -> SparkSession:
        return self.__spark

    def get_result(self) -> FileType:
        return self.__result_type
