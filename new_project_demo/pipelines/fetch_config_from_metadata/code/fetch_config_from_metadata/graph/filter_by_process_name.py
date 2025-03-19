from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from fetch_config_from_metadata.config.ConfigStore import *
from fetch_config_from_metadata.functions import *

def filter_by_process_name(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("process_name") == lit(Config.process_name)))
