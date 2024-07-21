from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pipeline_across_fabric.config.ConfigStore import *
from pipeline_across_fabric.functions import *

def catalog_api_input(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("method", StringType(), True), StructField("coin", StringType(), True), StructField("currency", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv(f"{Config.api_input_path}/api_input.csv")
