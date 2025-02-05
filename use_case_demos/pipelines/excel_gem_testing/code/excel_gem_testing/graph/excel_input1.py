from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_gem_testing.config.ConfigStore import *
from excel_gem_testing.functions import *

def excel_input1(spark: SparkSession) -> DataFrame:
    return spark.read.format("excel").option("header", True).option("dataAddress", "A1").load("hkj")
