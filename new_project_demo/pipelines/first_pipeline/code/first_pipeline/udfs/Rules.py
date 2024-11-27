from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *

@execute_rule
def new_rule(def: Column=lambda: col("def"), zxc: Column=lambda: col("zxc")):
    return when(((def > lit(5)) & (zxc < lit(10))), lit(5)).otherwise(lit(None)).alias("abc")
