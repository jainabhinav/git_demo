from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs import *

def write_kafka(spark: SparkSession, in0: DataFrame):
    import os
    df1 = in0.select(col("adasd").alias("key"), to_json(struct("*")).alias("value"))
    df2 = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df2.write\
        .format("kafka")\
        .option("kafka.sasl.mechanism", "PLAIN")\
        .option("kafka.security.protocol", "SASL_SSL")\
        .option(
          "kafka.sasl.jaas.config",
          (
            f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
            + f' required username="{os.environ["asd"]}" password="{os.environ["asfasf"]}";'
          )
        )\
        .option("kafka.bootstrap.servers", "asd")\
        .option("topic", "asd")\
        .save()
