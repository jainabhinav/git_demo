from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Source_0(spark: SparkSession) -> DataFrame:
    return spark.read\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/abhinav+1@simpledatalabs.com/OrdersDatasetInput.csv")
