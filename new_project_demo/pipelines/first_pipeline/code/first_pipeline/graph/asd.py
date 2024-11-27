from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs import *

def asd(spark: SparkSession) -> DataFrame:
    from delta.tables import DeltaTable
    from pyspark.sql.utils import AnalysisException
    import json

    try:
        desc_table = spark.sql("describe formatted sad")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        offset_dict = {}

        for row in DeltaTable.forName(spark, "sad").toDF().collect():
            if row["topic"] in offset_dict.keys():
                offset_dict[row["topic"]].update({row["partition"] : row["max_offset"] + 1})
            else:
                offset_dict[row["topic"]] = {
row["partition"] : row["max_offset"] + 1}

        return (spark.read\
            .format("kafka")\
            .options(
              **{
                "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"jasd\" password=\"asd\";",
                "kafka.sasl.mechanism": "SCRAM-SHA-256",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.bootstrap.servers": "asd",
                "kafka.session.timeout.ms": "6000",
                "group.id": "asd",
                "subscribe": "asd",
                "startingOffsets": json.dumps(offset_dict),
              }
            )\
            .load()\
            .withColumn("value", col("value").cast("string"))\
            .withColumn("key", col("key").cast("string")))
    else:
        return (spark.read\
            .format("kafka")\
            .options(
              **{
                "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"jasd\" password=\"asd\";",
                "kafka.sasl.mechanism": "SCRAM-SHA-256",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.bootstrap.servers": "asd",
                "kafka.session.timeout.ms": "6000",
                "group.id": "asd",
                "subscribe": "asd",
              }
            )\
            .load()\
            .withColumn("value", col("value").cast("string"))\
            .withColumn("key", col("key").cast("string")))
