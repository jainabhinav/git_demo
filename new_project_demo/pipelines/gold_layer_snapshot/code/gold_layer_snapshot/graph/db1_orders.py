from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *

def db1_orders(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.db_name}`.`orders`")
