from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def scd2_custom_target_1(spark: SparkSession) -> DataFrame:
    return spark.read.table("`abhinav_demo`.`scd2_test_custom_gem_1`")
