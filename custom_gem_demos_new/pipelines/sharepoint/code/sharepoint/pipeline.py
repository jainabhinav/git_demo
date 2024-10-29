from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sharepoint.config.ConfigStore import *
from sharepoint.functions import *
from prophecy.utils import *
from sharepoint.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sharepoint_customers_csv = sharepoint_customers_csv(spark)
    df_customer_info_reformat = customer_info_reformat(spark, df_sharepoint_customers_csv)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("sharepoint")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sharepoint")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/sharepoint", config = Config)(pipeline)

if __name__ == "__main__":
    main()
