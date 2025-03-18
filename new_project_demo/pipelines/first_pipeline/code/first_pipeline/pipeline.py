from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from first_pipeline.config.ConfigStore import *
from first_pipeline.udfs import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/first_pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/first_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
