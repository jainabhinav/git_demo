from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *

def orders_latest(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`abhinav_demo`.`orders_snapshot`")
