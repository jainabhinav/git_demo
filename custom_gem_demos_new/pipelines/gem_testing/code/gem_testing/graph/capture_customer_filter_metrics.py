from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def capture_customer_filter_metrics(spark: SparkSession, in0: DataFrame) -> DataFrame:
    print("Metrics capture start for: customer_filter_count")
    in0_count = in0.count()
    print("count: " + str(in0_count))
    in0.show(20)
    print("Metrics capture end for: customer_filter_count")

    return in0
