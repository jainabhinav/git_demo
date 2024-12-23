from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def reformat_json_parsed_content(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("id"), 
        col("name"), 
        col("age"), 
        col("json_string"), 
        col("json_parsed_content").cast(StringType()).alias("json_parsed_content"), 
        col("json_parsed_content.age"), 
        col("json_parsed_content.id"), 
        col("json_parsed_content.address.city").alias("city")
    )
