from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *

def mask_columns_sha1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("first_name", sha1("first_name")).withColumn("email", sha1("email"))
