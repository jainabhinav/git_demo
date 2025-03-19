from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from metadata_select.config.ConfigStore import *
from metadata_select.functions import *
from prophecy.utils import *
from metadata_select.graph import *

def pipeline(spark: SparkSession) -> None:
    df_metadata_columns = metadata_columns(spark)
    df_filter_customers_source = filter_customers_source(spark, df_metadata_columns)
    df_customers = customers(spark)
    df_reformat_metadata = reformat_metadata(spark, df_customers, df_filter_customers_source)
    customers_metadata_target(spark, df_reformat_metadata)
    df_customers_metadata_target_1 = customers_metadata_target_1(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("metadata_select")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/metadata_select")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/metadata_select", config = Config)(pipeline)

if __name__ == "__main__":
    main()
