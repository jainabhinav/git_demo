from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def parse_sample_record(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import xml_parse

    return xml_parse(
        in0,
        "xml_column",
        "parseFromSampleRecord",
        """<root>
  <person>
    <id>1</id>
    <name>
      <first>John</first>
      <last>Doe</last>
    </name>
    <address>
      <street>Main St</street>
      <city>Springfield</city>
      <zip>12345</zip>
    </address>
  </person>
</root>""",
        None
    )
