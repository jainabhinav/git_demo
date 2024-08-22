from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def customer_catalog_scd2(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `abhinav_demo`.`customer_scd2_new`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable, DeltaMergeBuilder
        updatesDF = in0
        updateColumns = updatesDF.columns
        existingTable = DeltaTable.forName(spark, "`abhinav_demo`.`customer_scd2_new`")
        existingDF = existingTable.toDF()
        cond = None
        scdHistoricColumns = ["first_name", "last_name", "phone", "email", "country_code", "account_open_date"]

        for scdCol in scdHistoricColumns:
            if cond is None:
                cond = (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol]))
            else:
                cond = (cond | (~ (existingDF[scdCol]).eqNullSafe(updatesDF[scdCol])))

        rowsToUpdate = existingDF.join(updatesDF, ["customer_id"], "left")
        print("rowsToUpdate: " + str(rowsToUpdate.count()))
        rowsToClose = rowsToUpdate\
                          .where((updatesDF["from_time"].isNull() & (existingDF["to_time"].isNull())))\
                          .select(*[existingDF[val] for val in updateColumns])
        print("rowsToClose: " + str(rowsToClose.count()))
        rowsToUpdateNewValues = rowsToUpdate\
                                    .where(
                                      (
                                        (existingDF["to_time"].isNull())
                                        & (~ updatesDF["from_time"].isNull())
                                        & (cond)
                                      )
                                    )\
                                    .select(*[updatesDF[val] for val in updateColumns])
        print("rowsToUpdateNewValues: " + str(rowsToUpdateNewValues.count()))
        stagedUpdatesDF = rowsToUpdateNewValues\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("customer_id")))\
                              .union(rowsToClose\
            .withColumn("from_time", expr("current_timestamp()"))\
            .withColumn("to_time", expr("current_timestamp()"))\
            .withColumn("mergeKey", concat("customer_id")))
        print("stagedUpdatesDF: " + str(stagedUpdatesDF.count()))
        updateCond = None

        for scdCol in scdHistoricColumns:
            if updateCond is None:
                updateCond = (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol]))
            else:
                updateCond = (updateCond | (~ (existingDF[scdCol]).eqNullSafe(stagedUpdatesDF[scdCol])))

        existingTable\
            .alias("existingTable")\
            .merge(
              stagedUpdatesDF.alias("staged_updates"),
              ((concat(existingDF["customer_id"]) == stagedUpdatesDF["mergeKey"]) & existingDF["to_time"].isNull())
            )\
            .whenMatchedUpdate(
              condition = (
                ((existingDF["to_time"].isNull()) & updateCond)
                | (stagedUpdatesDF["to_time"] == stagedUpdatesDF["from_time"])
              ),
              set = {
"to_time" : "staged_updates.from_time"}
            )\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").saveAsTable("`abhinav_demo`.`customer_scd2_new`")
