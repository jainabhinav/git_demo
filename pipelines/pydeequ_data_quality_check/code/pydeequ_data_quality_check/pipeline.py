from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pydeequ_data_quality_check.config.ConfigStore import *
from pydeequ_data_quality_check.functions import *
from prophecy.utils import *
from pydeequ_data_quality_check.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dummy_data = dummy_data(spark)
    df_data_quality_checks_out0, df_data_quality_checks_out1 = data_quality_checks(spark, df_dummy_data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("pydeequ_data_quality_check")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pydeequ_data_quality_check")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pydeequ_data_quality_check", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
