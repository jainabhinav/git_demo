from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from metadata_select.config.ConfigStore import *
from metadata_select.functions import *

def metadata_columns(spark: SparkSession) -> DataFrame:
    return spark.read.table("`abhinav_demo`.`metadata_reformat`")
