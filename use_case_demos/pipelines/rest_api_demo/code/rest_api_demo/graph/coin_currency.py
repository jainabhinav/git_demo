from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from rest_api_demo.config.ConfigStore import *
from rest_api_demo.functions import *

def coin_currency(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("coin"), 
        col("currency"), 
        col("content_parsed.rate").alias("rate"), 
        col("content_parsed.time").alias("time"), 
        current_timestamp().alias("insert_ts")
    )
