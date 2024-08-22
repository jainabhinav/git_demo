from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def flatten_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.columns
    selectCols = [col("key") if "key" in flt_col else col("key"),                   col("value-ordertime") if "value-ordertime" in flt_col else col("value.ordertime").alias("value-ordertime"),                   col("value-orderid") if "value-orderid" in flt_col else col("value.orderid").alias("value-orderid"),                   col("value-itemid") if "value-itemid" in flt_col else col("value.itemid").alias("value-itemid"),                   col("value-address-city") if "value-address-city" in flt_col else col("value.address.city")\
                    .alias("value-address-city"),                   col("value-address-state") if "value-address-state" in flt_col else col("value.address.state")\
                    .alias("value-address-state"),                   col("value-address-zipcode") if "value-address-zipcode" in flt_col else col("value.address.zipcode")\
                    .alias("value-address-zipcode"),                   col("timestamp") if "timestamp" in flt_col else col("timestamp")]

    return in0.select(*selectCols)
