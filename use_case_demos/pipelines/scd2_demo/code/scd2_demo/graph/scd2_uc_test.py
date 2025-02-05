from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def scd2_uc_test(spark: SparkSession, in0: DataFrame):
    from pyspark.sql.utils import AnalysisException

    try:
        desc_table = spark.sql("describe formatted `hive_metastore`.`abhinav_demo`.`scd2_uc_test23`")
        table_exists = True
    except AnalysisException as e:
        table_exists = False

    if table_exists:
        from delta.tables import DeltaTable, DeltaMergeBuilder
        updatesDF = in0.withColumn("is_min", lit("1")).withColumn("is_max", lit("1"))
        updateColumns = updatesDF.columns
        existingTable = DeltaTable.forName(spark, "`hive_metastore`.`abhinav_demo`.`scd2_uc_test23`")
        existingDF = existingTable.toDF()
        cond = None
        scdHistoricColumns = ["first_name", "phone", "email", "last_name", "country_code"]

        for scdCol in scdHistoricColumns:
            if cond is None:
                cond = (~ (col("existingDF." + scdCol)).eqNullSafe(col("updatesDF." + scdCol)))
            else:
                cond = (cond | (~ (col("existingDF." + scdCol)).eqNullSafe(col("updatesDF." + scdCol))))

        stagedUpdatesDF = updatesDF\
                              .alias("updatesDF")\
                              .join(existingDF.alias("existingDF"), ["customer_id"])\
                              .filter((col("existingDF.is_max") == lit("1")) & (cond))\
                              .select(*[col("updatesDF." + val) for val in updateColumns])\
                              .withColumn("is_min", lit("0"))\
                              .withColumn("mergeKey", lit(None))\
                              .union(updatesDF.withColumn("mergeKey", concat("customer_id")))
        updateCond = None

        for scdCol in scdHistoricColumns:
            if updateCond is None:
                updateCond = (~ (col("existingDF." + scdCol)).eqNullSafe(col("staged_updates." + scdCol)))
            else:
                updateCond = (
                    updateCond
                    | (~ (col("existingDF." + scdCol)).eqNullSafe(col("staged_updates." + scdCol)))
                )

        colsToInsert = [c for c in stagedUpdatesDF.columns if c != "mergeKey"]
        colsToInsertDict = {}

        for colName in colsToInsert:
            colsToInsertDict[colName] = "staged_updates." + colName

        existingTable\
            .alias("existingDF")\
            .merge(
              stagedUpdatesDF.alias("staged_updates"),
              concat(col("existingDF.customer_id")) == col("staged_updates.mergeKey")
            )\
            .whenMatchedUpdate(
              condition = (col("existingDF.is_max") == lit("1")) & updateCond,
              set = {
"is_max" : "0", "to_timestamp" : "staged_updates.from_timestamp"}
            )\
            .whenNotMatchedInsert(values = colsToInsertDict)\
            .execute()
    else:
        in0.write.format("delta").mode("overwrite").saveAsTable("`hive_metastore`.`abhinav_demo`.`scd2_uc_test23`")
