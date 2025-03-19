from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from employee_heirarchy.config.ConfigStore import *
from employee_heirarchy.functions import *
from prophecy.utils import *
from employee_heirarchy.graph import *

def pipeline(spark: SparkSession) -> None:
    df_employee = employee(spark)
    df_employee = df_employee.cache()
    df_multi_join = multi_join(
        spark, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee, 
        df_employee
    )
    df_reformatted_hierarchy_data = reformatted_hierarchy_data(spark, df_multi_join)
    employee_heirarchy(spark, df_reformatted_hierarchy_data)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("employee_heirarchy").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/employee_heirarchy")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/employee_heirarchy", config = Config)(pipeline)

if __name__ == "__main__":
    main()
