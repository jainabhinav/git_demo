from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *

def xlsx_test_sheet2(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("excel")\
        .option("header", True)\
        .option("dataAddress", "A1")\
        .load("dbfs:/Prophecy/abhinav/test_excel/test1.xlsx")
