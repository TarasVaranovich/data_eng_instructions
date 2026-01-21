from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, BooleanType

OPERATOR = StructType([
    StructField("operator_id", IntegerType(), False),
    StructField("operator_natural_key", IntegerType(), False),
    StructField("team_id", IntegerType(), False),
    StructField("primary_shift_id", IntegerType(), False),
    StructField("primary_line_factory_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("skill_level", StringType(), False),

    StructField("hire_date", DateType(), False),
    StructField("overtime_eligible", BooleanType(), False),
    StructField("hourly_rate_eur", FloatType(), False),
    StructField("reliability_score", FloatType(), False)
])

OPERATOR_CERTIFICATION = StructType([
    StructField("operator_id", IntegerType(), False),
    StructField("certification_id", IntegerType(), False)
])

CERTIFICATION = StructType([
    StructField("certification_id", IntegerType(), False),
    StructField("certification_natural_key", StringType(), False)
])

TEAM = StructType([
    StructField("team_id", IntegerType(), False),
    StructField("team_name", StringType(), False)
])