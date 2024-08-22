from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def filter_customer_id_range(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("customer_id") > lit(98)))
