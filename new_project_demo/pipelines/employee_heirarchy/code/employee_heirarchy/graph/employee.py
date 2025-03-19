from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from employee_heirarchy.config.ConfigStore import *
from employee_heirarchy.functions import *

def employee(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("AUX_SRC_SYS", IntegerType(), True), StructField("AUX_INS_DTM", StringType(), True), StructField("AUX_SRC_STATUS", StringType(), True), StructField("POS_NUM", IntegerType(), True), StructField("POS_TITLE", StringType(), True), StructField("USER_ID", IntegerType(), True), StructField("EMPL_ID", IntegerType(), True), StructField("EMPL_NAME", StringType(), True), StructField("FM_POS_NUM", IntegerType(), True), StructField("FM_USER_ID", IntegerType(), True), StructField("FM_EMPL_ID", IntegerType(), True), StructField("EM_POS_NUM", IntegerType(), True), StructField("EM_USER_ID", IntegerType(), True), StructField("EM_EMPL_ID", IntegerType(), True), StructField("DDAY_REPORTING_KEY", IntegerType(), True), StructField("FM_FLG", IntegerType(), True), StructField("EM_FLG", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("/Volumes/abhinav_demos/demos/test_volume/emp_test.csv")
