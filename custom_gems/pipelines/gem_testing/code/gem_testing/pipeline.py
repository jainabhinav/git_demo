from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from gem_testing.config.ConfigStore import *
from gem_testing.udfs.UDFs import *
from prophecy.utils import *
from gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_Masking_1 = Masking_1(spark, df_customers)

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
