from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sharepoint.config.ConfigStore import *
from sharepoint.functions import *

def customer_info_reformat(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(1).alias("asd"), 
        col("customer_id"), 
        col("customer_id"), 
        col("first_name"), 
        col("last_name"), 
        col("phone"), 
        col("email"), 
        col("country_code"), 
        col("account_open_date"), 
        col("account_flags")
    )
