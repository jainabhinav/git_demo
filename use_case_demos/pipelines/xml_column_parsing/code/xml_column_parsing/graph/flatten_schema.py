from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def flatten_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.columns
    selectCols = [col("xml_column") if "xml_column" in flt_col else col("xml_column"),                   col("id") if "id" in flt_col else col("id"),                   col("xml_parsed_content-person-id") if "xml_parsed_content-person-id" in flt_col else col("xml_parsed_content.person.id")\
                    .alias("xml_parsed_content-person-id"),                   col("xml_parsed_content-person-name-first") if "xml_parsed_content-person-name-first" in flt_col else col("xml_parsed_content.person.name.first")\
                    .alias("xml_parsed_content-person-name-first"),                   col("xml_parsed_content-person-name-last") if "xml_parsed_content-person-name-last" in flt_col else col("xml_parsed_content.person.name.last")\
                    .alias("xml_parsed_content-person-name-last"),                   col("xml_parsed_content-person-address-street") if "xml_parsed_content-person-address-street" in flt_col else col("xml_parsed_content.person.address.street")\
                    .alias("xml_parsed_content-person-address-street"),                   col("xml_parsed_content-person-address-city") if "xml_parsed_content-person-address-city" in flt_col else col("xml_parsed_content.person.address.city")\
                    .alias("xml_parsed_content-person-address-city"),                   col("xml_parsed_content-person-address-zip") if "xml_parsed_content-person-address-zip" in flt_col else col("xml_parsed_content.person.address.zip")\
                    .alias("xml_parsed_content-person-address-zip")]

    return in0.select(*selectCols)
