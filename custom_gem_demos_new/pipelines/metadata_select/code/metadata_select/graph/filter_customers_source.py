from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from metadata_select.config.ConfigStore import *
from metadata_select.functions import *

def filter_customers_source(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("source_name") == lit("customers")))
