from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def capture_metrics(spark: SparkSession, in0: DataFrame) -> DataFrame:
    print("Metrics capture start for: customers_source")
    in0_count = in0.count()
    print("count: " + str(in0_count))
    print("Metrics capture end for: customers_source")

    return in0
