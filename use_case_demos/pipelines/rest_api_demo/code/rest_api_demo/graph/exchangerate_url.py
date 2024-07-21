from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from rest_api_demo.config.ConfigStore import *
from rest_api_demo.functions import *

def exchangerate_url(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("method"), 
        col("coin"), 
        col("currency"), 
        concat(lit("https://rest.coinapi.io/v1/exchangerate/"), col("coin"), lit("/"), col("currency")).alias("url")
    )
