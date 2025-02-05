from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from excel_gem_testing.config.ConfigStore import *
from excel_gem_testing.functions import *
from prophecy.utils import *
from excel_gem_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_excel_input = excel_input(spark)
    excel_output(spark, df_excel_input)
    df_excel_input1 = excel_input1(spark)
    excel_output1(spark, df_excel_input1)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("excel_gem_testing").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/excel_gem_testing")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/excel_gem_testing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
