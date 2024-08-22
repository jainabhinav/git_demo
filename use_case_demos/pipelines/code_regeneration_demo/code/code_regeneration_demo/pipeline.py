from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *
from prophecy.utils import *
from code_regeneration_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_name_transformation = name_transformation(spark, df_customers)
    df_mask_columns_sha1 = mask_columns_sha1(spark, df_customers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("code_regeneration_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/code_regeneration_demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/code_regeneration_demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
