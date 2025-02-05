from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming_column_parser.config.ConfigStore import *
from kafka_streaming_column_parser.functions import *

def reformat_json_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("key"), 
        col("json_parsed_content.orderid").alias("orderid"), 
        col("json_parsed_content.xml").alias("xml")
    )
