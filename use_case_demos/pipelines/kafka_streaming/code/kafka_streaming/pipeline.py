from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

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
