from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *
from prophecy.utils import *
from gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_distribute_rows_out0, df_distribute_rows_out1 = distribute_rows(spark, df_customers)
    df_reformatted_customers_1 = reformatted_customers_1(spark, df_distribute_rows_out1)
    empty_script(spark)
    df_reformatted_customers = reformatted_customers(spark, df_distribute_rows_out0)
    df_SetOperation_1 = SetOperation_1(spark, df_reformatted_customers, df_reformatted_customers_1)
    customer_catalog_scd2(spark, df_SetOperation_1)
    df_customer_catalog_scd2_1 = customer_catalog_scd2_1(spark)
    df_filter_by_customer_id = filter_by_customer_id(spark, df_customer_catalog_scd2_1)
    df_customers_1 = customers_1(spark)
    df_filter_customer_id_range = filter_customer_id_range(spark, df_customers_1)
    df_filter_by_customer_id_1 = filter_by_customer_id_1(spark, df_customer_catalog_scd2_1)
    restore_delta_table_version(spark)
    df_reformatted_customers_2 = reformatted_customers_2(spark, df_filter_customer_id_range)
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
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gem_testing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
