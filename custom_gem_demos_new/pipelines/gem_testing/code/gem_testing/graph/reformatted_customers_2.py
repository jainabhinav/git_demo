from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def reformatted_customers_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        when((col("customer_id") == lit(100)), lit(102)).otherwise(col("customer_id")).alias("customer_id"), 
        col("first_name"), 
        upper(col("last_name")).alias("last_name"), 
        col("phone"), 
        col("email"), 
        col("country_code"), 
        col("account_open_date"), 
        col("account_flags"), 
        current_timestamp().alias("from_time"), 
        lit(None).cast(TimestampType()).alias("to_time")
    )
