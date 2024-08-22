from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def streaming_kafka(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils
    consumer_options = {
        "kafka.sasl.jaas.config": (
          f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
          + f' required username="{DBUtils(spark).secrets.get(scope = "abhinav_demo", key = "kafka_api_key")}" password="{DBUtils(spark).secrets.get(scope = "abhinav_demo", key = "kafka_api_secret")}";'
        ),
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.bootstrap.servers": "pkc-12576z.us-west2.gcp.confluent.cloud:9092",
        "kafka.session.timeout.ms": "6000",
        "group.id": "",
    }
    consumer_options["subscribe"] = "demo_topic"
    consumer_options["startingOffsets"] = "latest"
    consumer_options["includeHeaders"] = False

    return (spark.readStream\
        .format("kafka")\
        .options(**consumer_options)\
        .load()\
        .withColumn("value", col("value").cast("string"))\
        .withColumn("key", col("key").cast("string")))
