from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *
from prophecy.utils import *
from gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_Reformat_1 = Reformat_1(spark, df_customers)
    df_mask_names = mask_names(spark, df_customers)
    Subgraph_1(spark, Config.Subgraph_1)

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
