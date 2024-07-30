from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from gem_testing.functions import *

def select_1(spark: SparkSession) -> DataFrame:
    out0 = spark.sql("select 1")

    return out0
