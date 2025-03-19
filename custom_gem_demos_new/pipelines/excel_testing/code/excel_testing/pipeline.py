from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *
from prophecy.utils import *
from excel_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    xlsx_target_test(spark, df_customers)
    df_xlsx_test = xlsx_test(spark)
    df_xlsx_test_sheet2 = xlsx_test_sheet2(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("excel_testing")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/excel_testing")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/excel_testing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
