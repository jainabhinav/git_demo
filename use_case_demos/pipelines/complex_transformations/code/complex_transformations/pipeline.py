from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from complex_transformations.config.ConfigStore import *
from complex_transformations.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("complex_transformations")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/complex_transformations")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/complex_transformations", config = Config)(pipeline)

if __name__ == "__main__":
    main()
