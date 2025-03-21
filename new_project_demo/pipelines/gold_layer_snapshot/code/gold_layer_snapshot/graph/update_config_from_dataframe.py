from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *

def update_config_from_dataframe(spark: SparkSession, in0: DataFrame):
    collect_arr = in0.collect()[0]
    print("current_snapshot: " + Config.current_snapshot)
    print("count_threshold: " + Config.count_threshold)
    print("null_fk_threshold: " + Config.null_fk_threshold)
    Config.count_threshold = str(collect_arr[1])
    Config.null_fk_threshold = str(collect_arr[2])
    Config.current_snapshot = str(collect_arr[3])
    print("current_snapshot: " + Config.current_snapshot)
    print("count_threshold: " + Config.count_threshold)
    print("null_fk_threshold: " + Config.null_fk_threshold)

    return 
