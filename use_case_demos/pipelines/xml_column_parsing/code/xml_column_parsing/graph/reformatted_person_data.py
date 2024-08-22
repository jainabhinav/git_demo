from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *

def reformatted_person_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("xml_parsed_content.person.id").alias("id"), 
        col("xml_parsed_content.person.name.first").alias("first_name"), 
        col("xml_parsed_content.person.name.last").alias("last_name"), 
        col("xml_parsed_content.person.address.city").alias("city"), 
        col("xml_parsed_content.person.address.zip").alias("zip")
    )
