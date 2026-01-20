from abc import ABC, abstractmethod
from typing import Any

from pyspark.sql.connect.session import SparkSession


class Pipeline(ABC):
    def __init__(self, spark: SparkSession):
        self._spark = spark

    @abstractmethod
    def run(self) -> Any:
        pass
