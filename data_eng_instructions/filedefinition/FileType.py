from enum import StrEnum


class FileType(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    DELTA = "delta"