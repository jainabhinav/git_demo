from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from iceberg_demo.config.ConfigStore import *
from iceberg_demo.functions import *

def iceberg_write(spark: SparkSession, in0: DataFrame):
    df1 = in0.writeTo("`hadoop_catalog_1`.`prophecy_doc_demo`.`employees_test`")
    df2 = df1.using("iceberg")
    df3 = df2.partitionedBy("Department")
    df4 = df3.tableProperty("write.spark.accept-any-schema", "true")
    df4.createOrReplace()
