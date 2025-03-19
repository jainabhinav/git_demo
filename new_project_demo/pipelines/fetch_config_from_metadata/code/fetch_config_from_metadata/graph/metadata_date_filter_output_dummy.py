from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from fetch_config_from_metadata.config.ConfigStore import *
from fetch_config_from_metadata.functions import *

def metadata_date_filter_output_dummy(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("overwriteSchema", True)\
        .mode("overwrite")\
        .saveAsTable("`hive_metastore`.`abhinav_demo`.`metadat_date_filter_output_dummy`")
