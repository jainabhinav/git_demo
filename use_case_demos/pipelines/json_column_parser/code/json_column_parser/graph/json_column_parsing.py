from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def json_column_parsing(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import json_parse

    return json_parse(
        in0,
        "json_string",
        "parseFromSchema",
        None,
        "STRUCT<\n  person: STRUCT<\n    id: INT,\n    name: STRUCT<\n      first: STRING,\n      last: STRING\n    >,\n    address: STRUCT<\n      street: STRING,\n      city: STRING,\n      zip: STRING\n    >\n  >\n>",
        40
    )
