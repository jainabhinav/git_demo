from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sharepoint.config.ConfigStore import *
from sharepoint.functions import *

def sharepoint_customers_csv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("/personal/rakesh_prophecy340_onmicrosoft_com/Documents/customers.csv")
