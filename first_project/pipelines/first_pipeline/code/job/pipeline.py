from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_third_dataset = third_dataset(spark)
    df_Source_0 = Source_0(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.sparkContext._jvm.org.apache.spark.sql.MetricsCollector.start(spark._jsparkSession, "")
    pipeline(spark)
    spark.sparkContext._jvm.org.apache.spark.sql.MetricsCollector.end()

if __name__ == "__main__":
    main()
