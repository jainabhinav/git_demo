from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.udfs.UDFs import *

def Masking_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("phone_sha1", sha1(col("phone")))\
        .withColumn("email_md5", md5(col("email")))\
        .withColumn("email_sha2", sha2(col("email"), 0))
