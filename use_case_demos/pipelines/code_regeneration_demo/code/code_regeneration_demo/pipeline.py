from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *
from prophecy.utils import *
from code_regeneration_demo.graph import *

def pipeline(spark: SparkSession) -> None:
    df_csv_source_test = csv_source_test(spark)
    df_csv_source_test = collectMetrics(
        spark, 
        df_csv_source_test, 
        "graph", 
        "1yemIvr6TtunCSbt9WHfz$$qcaKN7qiEGssIdog0emO5", 
        "SeUovy-08yljkKSQgO9aI$$NMEDmP9zQSTsIZsyQX4f4"
    )
    df_csv_source_test.cache().count()
    df_csv_source_test.unpersist()
    df_customers = customers(spark)
    df_customers = collectMetrics(
        spark, 
        df_customers, 
        "graph", 
        "EyTDdpvLY6adrE9-rVku3$$yycLyK-JeiKsEpog0dSmA", 
        "KmCYSf4uph8kL_ldfZTZ3$$_wr0fMIFdS_IYjld65apX"
    )
    df_customer_details_selection = customer_details_selection(spark, df_customers)
    df_customer_details_selection = collectMetrics(
        spark, 
        df_customer_details_selection, 
        "graph", 
        "qBk9bM6A2CsHOHr0Ym4Qz$$Vl5RBnSFrcrJ7_L-e_8kc", 
        "dywHGb-f2FSNauFFcTCPY$$1bv_4JCoOMtjFMsK3A1lo"
    )
    df_customer_details_selection.cache().count()
    df_customer_details_selection.unpersist()
    df_csv_source_test2 = csv_source_test2(spark)
    df_csv_source_test2 = collectMetrics(
        spark, 
        df_csv_source_test2, 
        "graph", 
        "H5AF9IJD4-korH7WWEOjr$$Ta0e8V-KAz4fTpBcDypkC", 
        "eLUOp4H0OdVppa5CGXhqK$$rXwuOMA9mG4aYYfHAWJWk"
    )
    df_csv_source_test2.cache().count()
    df_csv_source_test2.unpersist()
    df_name_transformation = name_transformation(spark, df_customers)
    df_name_transformation = collectMetrics(
        spark, 
        df_name_transformation, 
        "graph", 
        "4YhY8rKm7v9JcuJ1wRYBf$$t0jm-2rkTMf7qw2IITtkR", 
        "Ah3H-toskmeZoCr0lGjk_$$rCg-SZJHx5T_QOWWuEyVT"
    )
    df_name_transformation.cache().count()
    df_name_transformation.unpersist()

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("code_regeneration_demo")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/code_regeneration_demo")
    registerUDFs(spark)
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/code_regeneration_demo", config = Config)(
        MetricsCollector.withSparkOptimisationsDisabled(pipeline)
    )

if __name__ == "__main__":
    main()
