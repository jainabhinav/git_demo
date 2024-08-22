from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def reformat_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("key"), 
        from_json(
            col("value"), 
            "STRUCT<\n  ordertime: LONG,\n  orderid: INT,\n  itemid: STRING,\n  address: STRUCT<\n    city: STRING,\n    state: STRING,\n    zipcode: INT\n  >\n>"
          )\
          .alias("value"), 
        col("timestamp")
    )
