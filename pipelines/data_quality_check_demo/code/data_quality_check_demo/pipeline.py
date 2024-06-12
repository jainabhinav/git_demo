from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_quality_check_demo.config.ConfigStore import *
from data_quality_check_demo.functions import *
from prophecy.utils import *
from data_quality_check_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers_data = customers_data(spark)
    df_data_quality_checks_out0, df_data_quality_checks_out1 = data_quality_checks(spark, df_customers_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("data_quality_check_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/data_quality_check_demo")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/data_quality_check_demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
