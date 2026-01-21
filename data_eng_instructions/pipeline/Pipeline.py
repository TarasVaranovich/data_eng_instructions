from abc import ABC, abstractmethod
from typing import Any

import data_eng_instructions.utils.path_utility as local_path
from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.pipeline.PipelineParam import PipelineParam
from data_eng_instructions.writer.EntityWriter import EntityWriter


class Pipeline(ABC):
    def __init__(self, param: PipelineParam):
        self._param = param

    @abstractmethod
    def run(self) -> Any:
        pass

    def write_to_storage(self, entity_writer: EntityWriter, f_name: str) -> Any:
        file_type: FileType = self._param.get_result_type()
        match file_type:
            case FileType.CSV:
                entity_writer.write_csv(f"csv/{f_name}.csv")
            case FileType.PARQUET:
                entity_writer.write_parquet(f"parquet/{f_name}.parquet")
            case _:
                raise NotImplementedError(f"No writer of type: {file_type}")

    @staticmethod
    def storage_path() -> str:
        root: str = local_path.project_root()
        storage_path: str = f"{root}/storage"
        return storage_path
