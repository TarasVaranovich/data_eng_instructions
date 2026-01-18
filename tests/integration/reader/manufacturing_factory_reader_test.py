from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.reader.ManufacturingFactoryReader import ManufacturingFactoryReader
from data_eng_instructions.filedefinition.ManufacturingFactoryDefinition import ManufacturingFactoryDefinition
from data_eng_instructions.service.session import make_csv_local

def test():
    session: SparkSession = make_csv_local()
    manufacturing_factory_definition: ManufacturingFactoryDefinition = ManufacturingFactoryDefinition()
    reader: ManufacturingFactoryReader = \
        ManufacturingFactoryReader(session, manufacturing_factory_definition)
    result: DataFrame = reader.read_csv_batch()
    assert result.isEmpty() is False
    result.show(10)
    session.stop()