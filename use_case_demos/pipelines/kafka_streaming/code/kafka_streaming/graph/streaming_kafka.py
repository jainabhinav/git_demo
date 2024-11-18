from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def streaming_kafka(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils

    return spark.read\
        .format("kafka")\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option(
          "kafka.sasl.jaas.config",
          (
            f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
            + f' required username="{DBUtils(spark).secrets.get(scope = "abhinav_demo", key = "kafka_api_key")}" password="{DBUtils(spark).secrets.get(scope = "abhinav_demo", key = "kafka_api_secret")}";'
          )
        )\
        .option("kafka.bootstrap.servers", "pkc-12576z.us-west2.gcp.confluent.cloud:9092")\
        .option("kafka.session.timeout.ms", "6000")\
        .option("subscribe", "demo_topic")\
        .option("startingOffsets", "latest")\
        .option("includeHeaders", False)\
        .load()\
        .withColumn("value", col("value").cast("string"))\
        .withColumn("key", col("key").cast("string"))
