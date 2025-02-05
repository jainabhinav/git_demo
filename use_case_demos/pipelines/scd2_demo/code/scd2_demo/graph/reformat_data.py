from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from scd2_demo.config.ConfigStore import *
from scd2_demo.functions import *

def reformat_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        when((length(col("first_name")) < lit(6)), concat(upper(col("first_name")), lit("12")))\
          .otherwise(col("first_name"))\
          .alias("first_name"), 
        col("last_name"), 
        col("phone"), 
        col("email"), 
        col("country_code"), 
        col("account_open_date"), 
        col("account_flags"), 
        current_timestamp().alias("from_timestamp"), 
        lit(None).cast(TimestampType()).alias("to_timestamp"), 
        lit(1).alias("is_min"), 
        lit(1).alias("is_max"), 
        lit(1).alias("new_col"), 
        lit(2).alias("new_col2")
    )
