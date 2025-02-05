from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def scd2_delta_target(spark: SparkSession, in0: DataFrame):
    from delta.tables import DeltaTable, DeltaMergeBuilder

    if (
        DeltaTable.isDeltaTable(spark, "dbfs:/Prophecy/abhinav/delta_test/test1")
        and spark._jvm.org.apache.hadoop.fs.FileSystem\
          .get(spark._jsc.hadoopConfiguration())\
          .exists(spark._jvm.org.apache.hadoop.fs.Path("dbfs:/Prophecy/abhinav/delta_test/test1"))
    ):
        updatesDF = in0.withColumn("is_min", lit("1")).withColumn("is_max", lit("1"))
        updateColumns = updatesDF.columns
        existingTable = DeltaTable.forPath(spark, "dbfs:/Prophecy/abhinav/delta_test/test1")
        existingDF = existingTable.toDF()
        cond = None
        scdHistoricColumns = ["first_name", "last_name", "phone", "email"]

        for scdCol in scdHistoricColumns:
            if cond is None:
                cond = (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol]))
            else:
                cond = (cond | (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol])))

        stagedUpdatesDF = updatesDF\
                              .join(existingDF, ["customer_id"])\
                              .where((existingDF["is_max"] == lit("1")) & (cond))\
                              .select(*[updatesDF[val] for val in updateColumns])\
                              .withColumn("is_min", lit("0"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("customer_id")))
        updateCond = None

        for scdCol in scdHistoricColumns:
            if updateCond is None:
                updateCond = (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol]))
            else:
                updateCond = (updateCond | (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol])))

        colsToInsert = [c for c in stagedUpdatesDF.columns if c != "mergeKey"]
        colsToInsertDict = {}

        for colName in colsToInsert:
            colsToInsertDict[colName] = "staged_updates." + colName

        existingTable\
            .alias("existingTable")\
            .merge(stagedUpdatesDF.alias("staged_updates"), concat(existingDF["customer_id"]) == stagedUpdatesDF["mergeKey"])\
            .whenMatchedUpdate(
              condition = (existingDF["is_max"] == lit("1")) & updateCond,
              set = {
"is_max" : "0", "to_timestamp" : "staged_updates.from_timestamp"}
            )\
            .whenNotMatchedInsert(values = colsToInsertDict)\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").save("dbfs:/Prophecy/abhinav/delta_test/test1")
