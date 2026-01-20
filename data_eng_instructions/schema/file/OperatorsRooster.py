from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, FloatType, ArrayType

OPERATORS_ROOSTER_CSV = StructType([
    StructField("operator_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("factory_id", StringType(), True),
    StructField("primary_line", StringType(), True),
    StructField("primary_shift", StringType(), True),
    StructField("skill_level", StringType(), True),
    StructField("certifications", StringType(), True),
    StructField("team", StringType(), True),
    StructField("hire_date", StringType(), True),
    StructField("overtime_eligible", StringType(), True),
    StructField("hourly_rate_eur", StringType(), True),
    StructField("reliability_score", StringType(), True)
])

OPERATORS_ROOSTER = StructType([
    StructField("operator_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("factory_id", StringType(), True),
    StructField("primary_line", StringType(), True),
    StructField("primary_shift", StringType(), True),
    StructField("skill_level", StringType(), True),
    StructField("certifications", ArrayType(StringType()), True),
    StructField("team", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("overtime_eligible", BooleanType(), True),
    StructField("hourly_rate_eur", FloatType(), True),
    StructField("reliability_score", FloatType(), True)
])