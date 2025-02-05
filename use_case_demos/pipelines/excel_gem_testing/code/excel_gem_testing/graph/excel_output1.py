from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_gem_testing.config.ConfigStore import *
from excel_gem_testing.functions import *

def excel_output1(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("excel")\
        .option("header", True)\
        .option("dataAddress", "A1")\
        .option("usePlainNumberFormat", False)\
        .mode("overwrite")\
        .save("noh")
