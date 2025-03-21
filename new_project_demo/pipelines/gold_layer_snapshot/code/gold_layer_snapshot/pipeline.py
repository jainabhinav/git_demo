from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *
from prophecy.utils import *
from gold_layer_snapshot.graph import *

def pipeline(spark: SparkSession) -> None:
    df_orders_threshold = orders_threshold(spark)
    update_config_from_dataframe(spark, df_orders_threshold)
    df_db2_orders = db2_orders(spark)
    df_orders_latest = orders_latest(spark)
    df_total_count = total_count(spark, df_orders_latest)
    df_orders_snapshot = orders_snapshot(spark)
    df_count_records = count_records(spark, df_orders_snapshot)
    df_full_cross_join = full_cross_join(spark, df_total_count, df_count_records)
    snapshot_counts(spark, df_full_cross_join)
    df_db1_orders = db1_orders(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("gold_layer_snapshot").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gold_layer_snapshot")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gold_layer_snapshot", config = Config)(pipeline)

if __name__ == "__main__":
    main()
