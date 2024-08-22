from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def xml_column_parser(spark: SparkSession, in0: DataFrame) -> DataFrame:

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
    df1 = in0.select("xml_column")
    df2 = df1.take(1)

    return in0.withColumn(
        "xml_parsed_content",
        expr("from_xml(xml_column, '{}')".format(infer_schema(ET.fromstring(df2[0][0]))))
    )
