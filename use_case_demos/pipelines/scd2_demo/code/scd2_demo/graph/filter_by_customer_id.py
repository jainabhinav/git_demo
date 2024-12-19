from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def filter_by_customer_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("customer_id") == lit("1")))
