from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from iceberg_demo.config.ConfigStore import *
from iceberg_demo.functions import *

def iceberg_read(spark: SparkSession) -> DataFrame:
    return spark.read.format("iceberg").load("`hadoop_catalog_1`.`prophecy_doc_demo`.`employees_test`")
