from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from fetch_config_from_metadata.config.ConfigStore import *
from fetch_config_from_metadata.functions import *

def metadata_date_filter(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`abhinav_demo`.`date_filter_table`")
