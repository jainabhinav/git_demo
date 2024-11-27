from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def ColumnParser_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import json_parse

    return json_parse(in0, "", "parseAuto", None, None, 40)
