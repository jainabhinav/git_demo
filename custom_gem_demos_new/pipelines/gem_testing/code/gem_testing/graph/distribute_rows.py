from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def distribute_rows(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("customer_id") <= lit(10)))
    df2 = in0.filter(((col("customer_id") > lit(10)) & (col("customer_id") < lit(90))))

    return df1, df2
