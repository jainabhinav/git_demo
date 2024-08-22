from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *
from prophecy.utils import *
from kafka_streaming.graph import *

def pipeline(spark: SparkSession) -> None:
    df_streaming_kafka = streaming_kafka(spark)
    df_reformat_data = reformat_data(spark, df_streaming_kafka)
    df_Filter_1 = Filter_1(spark, df_reformat_data)
    df_flatten_schema = flatten_schema(spark, df_Filter_1)
    streaming_kafka_target(spark, df_flatten_schema)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("kafka_streaming")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/kafka_streaming")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/kafka_streaming", config = Config)(pipeline)
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
