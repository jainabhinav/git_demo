from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def create_xml(spark: SparkSession) -> DataFrame:
    
    xml_example_1 = """<root>
             <person>
               <id>1</id>
               <name>
                 <last>Doe</last>
               </name>
               <address>
                 <street> main </street>
                 <city> springfield <city2>abc</city2></city>
               </address>
             </person>
           </root>"""
    xml_example_2 = """<root>
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
           </root>"""
    xml_data = [
(xml_example_1, ), (xml_example_2, )]
    out0 = spark.createDataFrame(xml_data, ["raw_xml"])

    return out0
