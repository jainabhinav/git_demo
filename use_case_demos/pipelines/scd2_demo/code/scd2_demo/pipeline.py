from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *
from prophecy.utils import *
from scd2_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_reformat_data = reformat_data(spark, df_customers)
    scd2_uc_test(spark, df_reformat_data)
    df_scd2_custom_target_1 = scd2_custom_target_1(spark)
    df_filter_by_customer_id = filter_by_customer_id(spark, df_scd2_custom_target_1)
    scd2_custom_target(spark, df_reformat_data)
    df_scd2_uc_test_1 = scd2_uc_test_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("scd2_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/scd2_demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/scd2_demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
