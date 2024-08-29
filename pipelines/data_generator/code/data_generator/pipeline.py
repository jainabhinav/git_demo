from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_generator.config.ConfigStore import *
from data_generator.functions import *
from prophecy.utils import *
from data_generator.graph import *

def pipeline(spark: SparkSession) -> None:
    df_data_generator = data_generator(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("data_generator")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/data_generator")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/data_generator", config = Config)(pipeline)

if __name__ == "__main__":
    main()
