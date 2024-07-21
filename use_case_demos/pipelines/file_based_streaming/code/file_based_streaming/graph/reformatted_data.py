from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from file_based_streaming.config.ConfigStore import *
from file_based_streaming.functions import *

def reformatted_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("id").cast(LongType()).alias("id"), 
        upper(col("name")).alias("name"), 
        col("age").cast(IntegerType()).alias("age"), 
        upper(col("department")).alias("department")
    )
