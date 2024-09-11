from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customer_orders.config.ConfigStore import *
from customer_orders.functions import *
from prophecy.utils import *
from customer_orders.graph import *

def pipeline(spark: SparkSession) -> None:
    df_orders = orders(spark)
    df_customers = customers(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("customer_orders")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customer_orders")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/customer_orders", config = Config)(pipeline)

if __name__ == "__main__":
    main()
