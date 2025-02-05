from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *

def xlsx_target_test(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("excel")\
        .option("header", True)\
        .option("dataAddress", "A1")\
        .option("usePlainNumberFormat", False)\
        .save("/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx")
