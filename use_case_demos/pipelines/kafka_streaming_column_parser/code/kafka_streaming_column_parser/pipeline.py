from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka_streaming_column_parser.config.ConfigStore import *
from kafka_streaming_column_parser.functions import *
from prophecy.utils import *
from kafka_streaming_column_parser.graph import *

def pipeline(spark: SparkSession) -> None:
    df_streaming_kafka = streaming_kafka(spark)
    df_json_parsing_with_inference = json_parsing_with_inference(spark, df_streaming_kafka)
    streaming_kafka_target(spark, df_json_parsing_with_inference)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("kafka_streaming_column_parser")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/kafka_streaming_column_parser")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/kafka_streaming_column_parser", config = Config)(
        pipeline
    )
    
    spark.streams.resetTerminated()
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
