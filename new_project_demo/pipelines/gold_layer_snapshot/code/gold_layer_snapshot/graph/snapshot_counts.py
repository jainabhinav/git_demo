from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_layer_snapshot.config.ConfigStore import *
from gold_layer_snapshot.functions import *

def snapshot_counts(spark: SparkSession, in0: DataFrame):
    collect_arr = in0.collect()[0]
    snapshot_count = collect_arr[0]
    null_fk_count = collect_arr[1]
    latest_count = collect_arr[2]
    count_comp = (latest_count - snapshot_count) / snapshot_count
    null_fk_comp = null_fk_count / latest_count

    if (count_comp <= float(Config.count_threshold) and null_fk_comp <= float(Config.null_fk_threshold)):
        print("view would be updated to latest")
        Config.current_snapshot = str(
            spark.sql("select max(version) from (describe history abhinav_demo.orders_snapshot)").collect()[0][0]
        )
    # if you use db then something like below can be done where db_name becomes configurable based on checks passing vs not
    # spark.sql(f"""CREATE OR REPLACE VIEW orders_snapshot_view as select * from abhinav_demo.orders_snapshot version as of {Config.current_snapshot}""")
    # code to update snaphot in metadat table
    else:
        print("view would not be updated")

    return 
