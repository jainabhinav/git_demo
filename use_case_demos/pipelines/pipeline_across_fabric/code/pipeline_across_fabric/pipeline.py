from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pipeline_across_fabric.config.ConfigStore import *
from pipeline_across_fabric.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("pipeline_across_fabric")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pipeline_across_fabric")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pipeline_across_fabric", config = Config)(pipeline)

if __name__ == "__main__":
    main()
