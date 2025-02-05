from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def streaming_kafka_target(spark: SparkSession, in0: DataFrame):
    in0.writeStream\
        .format("delta")\
        .option("checkpointLocation", "dbfs:/FileStore/data_engg/abhinav/kafka_demo_checkpoint_new1")\
        .queryName("StreamingTarget_1_nitWssceuYyn9DmASmVlS$$TX2_BpZrc_7XEsaMvmETX")\
        .outputMode("append")\
        .toTable("abhinav_demo.kafka_target_new_7_xml")
