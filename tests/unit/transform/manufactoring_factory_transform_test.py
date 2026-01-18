from pyspark.sql import SparkSession

from data_eng_instructions.transform.ManufacturingFactoryTransform import csv_to_type, filter_out_invalid_defects_definitions
from data_eng_instructions.type.ManufacturingFactory import MANUFACTURING_FACTORY_CSV

valid_row = [
    {
        "timestamp": "2025-11-03 06:00:00",
        "factory_id": "FRA-PLANT-01",
        "line_id": "Line-A",
        "shift": "Shift-1",
        "product_id": "P-Widget",
        "order_id": "WO-20251103-1860",
        "planned_qty": 47,
        "produced_qty": 41,
        "scrap_qty": 1,
        "defects_count": 0,
        "defect_type": None,
        "cycle_time_s": 44.4,
        "oee": 0.986,
        "availability": 1.0,
        "performance": 1.033,
        "quality": 0.954,
        "machine_state": "Running",
        "downtime_reason": None,
        "maintenance_type": "Corrective",
        "maintenance_due_date": "2025-11-05",
        "vibration_mm_s": 1.814,
        "temperature_c": 40.02,
        "pressure_bar": 6.36,
        "energy_kwh": 11.31,
        "operator_id": "OP-015",
        "workorder_status": "In-Progress"
    }
]

defect_example = [
    {
        "timestamp": "2025-11-03 07:15:00",
        "factory_id": "FRA-PLANT-01",
        "line_id": "Line-C",
        "shift": "Shift-1",
        "product_id": "P-Gadget",
        "order_id": "WO-20251103-8509",
        "planned_qty": 45,
        "produced_qty": 43,
        "scrap_qty": 2,
        "defects_count": 2,
        "defect_type": None,
        "cycle_time_s": 34.15,
        "oee": 0.975,
        "availability": 1.0,
        "performance": 1.037,
        "quality": 0.941,
        "machine_state": "Running",
        "downtime_reason": None,
        "maintenance_type": "Preventive",
        "maintenance_due_date": "2025-11-04",
        "vibration_mm_s": 1.704,
        "temperature_c": 36.98,
        "pressure_bar": 7.0,
        "energy_kwh": 11.267,
        "operator_id": "OP-017",
        "workorder_status": "In-Progress"
    }
]

def test_transform_csv_to_type():
    spark = SparkSession.builder.appName("Testing CSV to Type transform").getOrCreate()


    df = spark.createDataFrame(valid_row, MANUFACTURING_FACTORY_CSV)
    result = df.transform(csv_to_type)
    # assert result.schema == MANUFACTURING_FACTORY
    result.show()

    assert result.count() == len(valid_row)


def test_filter_out_invalid_defects_definitions():
    spark = SparkSession.builder.appName("Testing Defects validation transform").getOrCreate()

    sample_data = valid_row + defect_example
    df = spark.createDataFrame(sample_data, MANUFACTURING_FACTORY_CSV)
    result = df.transform(filter_out_invalid_defects_definitions)
    assert result.schema == MANUFACTURING_FACTORY_CSV
    result.show()

    assert result.count() == len(valid_row)
