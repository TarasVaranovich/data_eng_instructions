from pandas.core.interchange.dataframe_protocol import DataFrame

from data_eng_instructions.type.ManufacturingFactory import MANUFACTURING_FACTORY_CSV, MANUFACTURING_FACTORY


def csv_to_type(dataframe: DataFrame) -> DataFrame:
    assert dataframe.schema == MANUFACTURING_FACTORY_CSV
    assert dataframe.schema == MANUFACTURING_FACTORY
    return dataframe
