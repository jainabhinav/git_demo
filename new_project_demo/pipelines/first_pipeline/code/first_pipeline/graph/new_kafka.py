from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs import *

def new_kafka(spark: SparkSession) -> DataFrame:
    reader = spark.read\
                 .format("kafka")\
                 .option("kafka.bootstrap.servers", "asd")\
                 .option("subscribe", "sad")\
                 .option("kafka.session.timeout.ms", "6000")
    from pyspark.sql.utils import AnalysisException
    import json

    try:
        desc_table = spark.sql("describe formatted asd")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable
        offset_dict = {}

        for row in DeltaTable.forName(spark, "asd").toDF().collect():
            if row["topic"] in offset_dict.keys():
                offset_dict[row["topic"]].update({row["partition"] : row["max_offset"] + 1})
            else:
                offset_dict[row["topic"]] = {
row["partition"] : row["max_offset"] + 1}

        reader = spark.read\
                     .format("kafka")\
                     .option("kafka.bootstrap.servers", "asd")\
                     .option("subscribe", "sad")\
                     .option("kafka.session.timeout.ms", "6000")\
                     .option("startingOffsets", json.dumps(offset_dict))

    return reader\
        .load()\
        .withColumn("value", col("value").cast("string"))\
        .withColumn("key", col("key").cast("string"))
