from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *

def csv_source_test(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("a", StringType(), True), StructField("b", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv("asdf")
