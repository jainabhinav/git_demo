from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def create_json_string_column(spark: SparkSession) -> DataFrame:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, struct, to_json
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    # Define schema for the dummy data
    schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)

    ])
    # Create a list of dummy data
    data = [
(1, "Alice", None), (2, "Bob", 25), (3, "Charlie", 35)]
    # Create a DataFrame with the dummy data
    df = spark.createDataFrame(data, schema)
    # Convert the DataFrame to include a JSON string column
    out0 = df.withColumn("json_string", to_json(struct([col(c) for c in df.columns])))

    return out0
