from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from fetch_config_from_metadata.config.ConfigStore import *
from fetch_config_from_metadata.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("fetch_config_from_metadata").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/fetch_config_from_metadata")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/fetch_config_from_metadata", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
