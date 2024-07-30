from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def mask_names(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("first_name", sha1("first_name"))\
        .withColumn("last_name", sha1("last_name"))\
        .withColumn("phone", sha1("phone"))\
        .withColumn("email", sha1("email"))
