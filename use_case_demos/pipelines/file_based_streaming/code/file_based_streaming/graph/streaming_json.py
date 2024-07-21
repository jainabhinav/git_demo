from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from file_based_streaming.config.ConfigStore import *
from file_based_streaming.functions import *

def streaming_json(spark: SparkSession) -> DataFrame:
    return spark.readStream\
        .format("json")\
        .schema(
          StructType([
            StructField("age", LongType(), True), StructField("department", StringType(), True), StructField("id", LongType(), True), StructField("name", StringType(), True)
        ])
        )\
        .load("dbfs:/Prophecy/abhinav@simpledatalabs.com/streaming_demos/data_files")
