from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("categorize_delay", categorize_delay)
    spark.udf.register("square", square)
    

    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass

@udf(returnType = StringType())
def categorize_delay(arrival_delay: int):
    if arrival_delay < 0:
        return "Early"
    elif 0 <= arrival_delay <= 15:
        return "On Time"
    elif 16 <= arrival_delay <= 60:
        return "Slight Delay"
    else:
        return "Major Delay"

@udf(returnType = IntegerType())
def square(value: int, xyz: int):
    return value * value
