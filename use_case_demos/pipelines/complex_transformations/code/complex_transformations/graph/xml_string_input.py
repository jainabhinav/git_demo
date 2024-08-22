from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from complex_transformations.config.ConfigStore import *
from complex_transformations.functions import *

def xml_string_input(spark: SparkSession) -> DataFrame:
    return spark.read.table("`abhinav_demo`.`xml_input`")
