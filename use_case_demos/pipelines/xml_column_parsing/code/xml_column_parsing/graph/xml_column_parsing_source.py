from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def xml_column_parsing_source(spark: SparkSession) -> DataFrame:
    return spark.read.table("`abhinav_demo`.`xml_input`")
