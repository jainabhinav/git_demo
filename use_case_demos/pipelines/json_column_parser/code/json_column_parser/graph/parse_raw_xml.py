from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def parse_raw_xml(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import xml_parse

    return xml_parse(in0, "raw_xml", "parseAuto", None, None)
