from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from metadata_select.config.ConfigStore import *
from metadata_select.functions import *

def customers_metadata_target(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable("`abhinav_demo`.`customers_metadata_target`")
