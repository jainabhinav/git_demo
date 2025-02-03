from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.udfs.UDFs import *

def Masking_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("phone_md5", md5(col("phone"))).withColumn("phone_sha2", sha2(col("phone"), 224))
