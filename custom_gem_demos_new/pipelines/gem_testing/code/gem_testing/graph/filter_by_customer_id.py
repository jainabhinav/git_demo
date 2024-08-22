from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def filter_by_customer_id(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("customer_id") < lit(13)) | (col("customer_id") > lit(90))))
