from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from rest_api_demo.config.ConfigStore import *
from rest_api_demo.functions import *
from prophecy.utils import *
from rest_api_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    if Config.pipeline_start:
        df_catalog_api_input = catalog_api_input(spark)
        df_exchangerate_url = exchangerate_url(spark, df_catalog_api_input)
        df_rest_api_enrichment = rest_api_enrichment(spark, df_exchangerate_url)
        df_coin_currency = coin_currency(spark, df_rest_api_enrichment)
        catalog_api_output(spark, df_coin_currency)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("rest_api_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/rest_api_demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/rest_api_demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
