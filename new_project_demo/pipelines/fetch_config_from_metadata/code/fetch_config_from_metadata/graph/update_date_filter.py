from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from fetch_config_from_metadata.config.ConfigStore import *
from fetch_config_from_metadata.functions import *

def update_date_filter(spark: SparkSession, in0: DataFrame):
    print("initial value of date filter: " + Config.date_filter)
    date_filter_from_df = str(in0.select("date_filter").collect()[0][0])
    Config.date_filter = date_filter_from_df
    print("override value of date filter: " + Config.date_filter)

    return 
