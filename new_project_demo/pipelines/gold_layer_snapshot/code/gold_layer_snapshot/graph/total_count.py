from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *

def total_count(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg(
        count(lit(1)).alias("latest_count"), 
        sum(when(col("customer_id").isNull(), lit(1)).otherwise(lit(0))).alias("null_fk_count")
    )
