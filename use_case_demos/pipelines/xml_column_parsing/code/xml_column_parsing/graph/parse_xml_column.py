from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def parse_xml_column(spark: SparkSession, in0: DataFrame) -> DataFrame:

    def infer_schema(element):

        if len(element) == 0:
            return "STRING"

        fields = []

        for child in element:
            field_name = child.tag
            field_type = infer_schema(child)
            fields.append(f"{field_name}: {field_type}")

        return f"STRUCT<{', '.join(fields)}>"

    import xml.etree.ElementTree as ET

    return in0.withColumn(
        "xml_parsed_content",
        expr(
          "from_xml(xml_column, '{}')".format(
            infer_schema(
              ET.fromstring(
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
</root>"""
              )
            )
          )
        )
    )
