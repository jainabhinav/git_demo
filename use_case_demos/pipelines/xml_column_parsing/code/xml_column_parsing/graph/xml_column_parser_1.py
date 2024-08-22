from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def xml_column_parser_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "xml_parsed_content",
        expr(
          "from_xml(xml_column, 'STRUCT< person: STRUCT< id: INT, name: STRUCT< first: STRING, last: STRING >, address: STRUCT< street: STRING, city: STRING, zip: STRING > > >')"
        )
    )
