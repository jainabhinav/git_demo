from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from gem_testing.config.ConfigStore import *
from gem_testing.udfs.UDFs import *
from prophecy.utils import *
from gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_deleted_customers = deleted_customers(spark)
    df_customers = customers(spark)
    df_Masking_1 = Masking_1(spark, df_customers)
    df_RestAPIEnrich_1 = RestAPIEnrich_1(spark)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_RestAPIEnrich_1)
    df_Reformat_1 = Reformat_1(spark, df_customers)
    df_Masking_2 = Masking_2(spark, df_customers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gem_testing")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/gem_testing", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/gem_testing")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
