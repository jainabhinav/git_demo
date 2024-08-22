from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def customer_catalog_scd2_1(spark: SparkSession) -> DataFrame:
    return spark.read.table("`abhinav_demo`.`customer_scd2_new`")
