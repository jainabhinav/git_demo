from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from iceberg_demo.config.ConfigStore import *
from iceberg_demo.functions import *
from prophecy.utils import *
from iceberg_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sample_data = sample_data(spark)
    iceberg_write(spark, df_sample_data)
    df_iceberg_read = iceberg_read(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("iceberg_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/iceberg_demo")
    spark.conf.set("spark.sql.catalog.hadoop_catalog_1", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.hadoop_catalog_1.type", "hadoop")
    spark.conf.set("spark.sql.catalog.hadoop_catalog_1.warehouse", "gs://bigquery-temp-demo/warehouse/hc_1/")
    spark.conf.set("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.hive.type", "hive")
    spark.conf.set(
        "spark.sql.catalog.hive.warehouse",
        "gs://gcs-bucket-service-77fe-b99175ec-a32b-448c-ac67-62325308cb3a/hive-warehouse/"
    )
    spark.conf.set("spark.sql.catalog.hive.uri", "thrift://10.91.64.30:9083")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/iceberg_demo", config = Config)(pipeline)

if __name__ == "__main__":
    main()
