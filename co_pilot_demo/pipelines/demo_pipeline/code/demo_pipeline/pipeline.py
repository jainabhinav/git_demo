from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from demo_pipeline.config.ConfigStore import *
from demo_pipeline.functions import *
from prophecy.utils import *
from demo_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_orders = orders(spark)
    df_customers = customers(spark)
    df_by_customer_id = by_customer_id(spark, df_orders, df_customers)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("demo_pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/demo_pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/demo_pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
