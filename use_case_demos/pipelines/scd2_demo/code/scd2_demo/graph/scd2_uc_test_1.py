from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def scd2_uc_test_1(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`abhinav_demo`.`scd2_uc_test23`")
