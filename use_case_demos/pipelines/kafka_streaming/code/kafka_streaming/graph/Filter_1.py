from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from kafka_streaming.config.ConfigStore import *
from kafka_streaming.functions import *

def Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
