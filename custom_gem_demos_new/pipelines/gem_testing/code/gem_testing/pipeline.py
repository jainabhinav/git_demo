from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *
from prophecy.utils import *
from gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_1 = customers_1(spark)

    if (Config.debug_flag == "true"):
        df_capture_metrics = capture_metrics(spark, df_customers_1)
    else:
        df_capture_metrics = df_customers_1

    df_filter_customer_id_range = filter_customer_id_range(spark, df_capture_metrics)

    if (Config.debug_flag == "true"):
        df_capture_customer_filter_metrics = capture_customer_filter_metrics(spark, df_filter_customer_id_range)
    else:
        df_capture_customer_filter_metrics = df_filter_customer_id_range

    df_reformatted_customers_2 = reformatted_customers_2(spark, df_capture_customer_filter_metrics)
    customer_catalog_scd2_2(spark, df_reformatted_customers_2)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("gem_testing")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gem_testing")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gem_testing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
