from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from soda_dq.config.ConfigStore import *
from soda_dq.functions import *
from prophecy.utils import *
from soda_dq.graph import *

def pipeline(spark: SparkSession) -> None:
    df_generate_customer_emails = generate_customer_emails(spark)
    df_soda_data_quality_checks = soda_data_quality_checks(spark, df_generate_customer_emails)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("soda_dq")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/soda_dq")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/soda_dq", config = Config)(pipeline)

if __name__ == "__main__":
    main()
