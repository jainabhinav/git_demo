from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from conditional_execution.config.ConfigStore import *
from conditional_execution.functions import *
from prophecy.utils import *
from conditional_execution.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)

    if df_customers > 0:
        scd2_custom_target(spark, df_customers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("conditional_execution")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/conditional_execution")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/conditional_execution", config = Config)(pipeline)

if __name__ == "__main__":
    main()
