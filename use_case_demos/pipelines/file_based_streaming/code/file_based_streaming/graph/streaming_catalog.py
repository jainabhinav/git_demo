from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from file_based_streaming.config.ConfigStore import *
from file_based_streaming.functions import *

def streaming_catalog(spark: SparkSession, in0: DataFrame):
    if spark.catalog._jcatalog.tableExists("abhinav_demo.streaming_json_demo"):
        from delta.tables import DeltaTable
        from prophecy.streaming.delta_lake_utils import deltaLakeMergeForEachBatch # noqa
        in0.writeStream\
            .format("delta")\
            .option("checkpointLocation", "dbfs:/FileStore/data_engg/abhinav/streaming_demo_checkpoint_new")\
            .queryName("StreamingTarget_1__qzabJRWfYIkFSksokLaW$$1Rt4ZG-G7lLcyb75nGTXM")\
            .foreachBatch(
              deltaLakeMergeForEachBatch(
                DeltaTable.forName(spark, "abhinav_demo.streaming_json_demo"),
                {
                  "writeMode": "merge",
                  "mergeTargetAlias": "target",
                  "mergeSourceAlias": "source",
                  "mergeCondition": (col("source.id") == col("target.id")),
                  "matchedActionDelete": "ignore",
                  "matchedConditionDelete": None,
                  "matchedAction": "update",
                  "matchedTable": {},
                  "notMatchedTable": {},
                  "matchedCondition": None,
                  "notMatchedAction": "insert",
                  "notMatchedCondition": None,
                  "keyColumns": [],
                  "historicColumns": [],
                  "fromTimeCol": None,
                  "toTimeCol": None,
                  "minFlagCol": None,
                  "maxFlagCol": None,
                  "flagValue": "integer",
                }
              )
            )\
            .start()
    else:
        in0.writeStream\
            .format("delta")\
            .option("checkpointLocation", "dbfs:/FileStore/data_engg/abhinav/streaming_demo_checkpoint_new")\
            .queryName("StreamingTarget_1__qzabJRWfYIkFSksokLaW$$1Rt4ZG-G7lLcyb75nGTXM")\
            .outputMode("merge")\
            .toTable("abhinav_demo.streaming_json_demo")
