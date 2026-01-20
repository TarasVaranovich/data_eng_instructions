from pandas.core.interchange.dataframe_protocol import DataFrame
from pyspark.sql.connect.session import SparkSession

from data_eng_instructions.filedefinition.operatorsrooster.source.OperatorsRoosterDefinitionSource import \
    OperatorsRoosterDefinitionSource
from data_eng_instructions.reader.OperatorsRoosterReader import OperatorsRoosterReader
from data_eng_instructions.service.session import make_csv_local


def test():
    session: SparkSession = make_csv_local()
    operators_rooster_definition: OperatorsRoosterDefinitionSource = OperatorsRoosterDefinitionSource()
    reader: OperatorsRoosterReader = \
        OperatorsRoosterReader(session, operators_rooster_definition)
    result: DataFrame = reader.read_batch()
    assert result.isEmpty() is False
    result.show(10)
    session.stop()
