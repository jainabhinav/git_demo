from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline_across_fabric.config.ConfigStore import *
from pipeline_across_fabric.functions import *

def exchangerate_url(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        upper(col("method")).alias("method"), 
        upper(col("coin")).alias("coin"), 
        upper(col("currency")).alias("currency"), 
        concat(lit("https://rest.coinapi.io/v1/exchangerate/"), col("coin"), lit("/"), col("currency")).alias("url")
    )
