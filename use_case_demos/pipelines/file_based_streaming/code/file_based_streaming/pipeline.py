from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from file_based_streaming.config.ConfigStore import *
from file_based_streaming.functions import *
from prophecy.utils import *
from file_based_streaming.graph import *

def pipeline(spark: SparkSession) -> None:
    df_streaming_json = streaming_json(spark)
    df_reformatted_data = reformatted_data(spark, df_streaming_json)
    streaming_catalog(spark, df_reformatted_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("file_based_streaming")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/file_based_streaming")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/file_based_streaming", config = Config)(pipeline)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
