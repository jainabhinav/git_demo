from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def parse_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import xml_parse

    return xml_parse(
        in0,
        "xml_column",
        "parseFromSchema",
        None,
        "STRUCT< person: STRUCT< id: INT, name: STRUCT< first: STRING, last: STRING >, address: STRUCT< street: STRING, city: STRING, zip: STRING > > >"
    )
