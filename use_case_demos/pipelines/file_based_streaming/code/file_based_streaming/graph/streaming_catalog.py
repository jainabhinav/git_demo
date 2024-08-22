from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from file_based_streaming.config.ConfigStore import *
from file_based_streaming.functions import *

def streaming_catalog(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("delta")\
        .option("checkpointLocation", "dbfs:/FileStore/data_engg/abhinav/streaming_demo_checkpoint_new_2")\
        .queryName("StreamingTarget_1__qzabJRWfYIkFSksokLaW$$1Rt4ZG-G7lLcyb75nGTXM")\
        .outputMode("append")\
        .toTable("abhinav_demo.streaming_json_demo_3")
