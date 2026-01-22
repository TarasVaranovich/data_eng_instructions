from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.filedefinition.FileType import FileType
from data_eng_instructions.filedefinition.manufctoringfactory.dwh.ManufacturingFactoryDefinitionDWH import \
    ManufacturingFactoryDefinitionDWH
from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.service.session import make_csv_local


def test_read_parquet():
    session: SparkSession = make_csv_local()
    manufacturing_factory_definition_dwh: ManufacturingFactoryDefinitionDWH = (
        ManufacturingFactoryDefinitionDWH(FileType.PARQUET))
    reader: ManufacturingFactoryReader = \
        ManufacturingFactoryReader(session, manufacturing_factory_definition_dwh)
    result: DataFrame = reader.read_batch()
    assert result.isEmpty() is False
    result.show(10)
    session.stop()

def test_read_csv():
    session: SparkSession = make_csv_local()
    manufacturing_factory_definition_dwh: ManufacturingFactoryDefinitionDWH = (
        ManufacturingFactoryDefinitionDWH(FileType.CSV))
    reader: ManufacturingFactoryReader = \
        ManufacturingFactoryReader(session, manufacturing_factory_definition_dwh)
    result: DataFrame = reader.read_batch()
    assert result.isEmpty() is False
    result.show(10)
    session.stop()
