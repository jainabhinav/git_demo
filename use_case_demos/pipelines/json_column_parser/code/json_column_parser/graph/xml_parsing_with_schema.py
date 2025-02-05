from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def xml_parsing_with_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:

    def xml_parse_new(in0, column_to_parse, parsingMethod, sampleRecord, schema):
        try:
            from pyspark.sql.functions import from_xml, schema_of_xml, lit
            from pyspark.sql.types import StructType

            if parsingMethod in ["parseFromSampleRecord", "parseAuto"]:

                if parsingMethod == "parseFromSampleRecord":
                    sample_xml = sampleRecord
                else:
                    sample_xml = in0.limit(1).select(column_to_parse).collect()[0][0]

                xml_schema = schema_of_xml(lit(sample_xml))
                output_df = in0.withColumn("xml_parsed_content", from_xml(column_to_parse, xml_schema))
            else:
                try:
                    xml_schema = schema
                    output_df = in0.withColumn(
                        "xml_parsed_content",
                        expr(f"from_xml({column_to_parse}, '{xml_schema}')")
                    )
                except Exception as e:
                    xml_schema = StructType.fromDDL(schema)
                    output_df = in0.withColumn("xml_parsed_content", from_xml(column_to_parse, xml_schema))

            return output_df
        except Exception as e:
            print(f"An error occurred while fetching data: {e}")
            raise e

    out0 = xml_parse_new(
        in0,
        "raw_xml",
        "parseAuto",
        """<root>
             <person>
               <id>1</id>
               <name>
                 <first>John</first>
                 <last>Doe</last>
               </name>
               <address>
                 <street> main </street>
                 <city> springfield <city2>abc</city2></city>
                 <zip>1343k</zip>
               </address>
             </person>
           </root>""",
        """STRUCT<
  person: STRUCT<
    id: STRING,
    name: STRUCT<
      first: STRING,
      last: STRING
    >,
    address: STRUCT<
      street: STRING,
      city: STRUCT<
        _VALUE: STRING,
        city2: STRING
      >,
      zip: STRING
    >
  >
>"""
    )

    return out0
