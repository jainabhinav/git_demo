from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def json_parsing_with_schema(spark: SparkSession, in0: DataFrame) -> DataFrame:

    def json_parse_new(in0, column_to_parse, parsingMethod, sampleRecord, schema, schemaInferCount):
        try:
            from pyspark.sql.functions import from_json, schema_of_json, lit
            from pyspark.sql.types import StructType

            if parsingMethod in ["parseFromSampleRecord", "parseAuto"]:
                if parsingMethod == "parseFromSampleRecord":
                    sample_json = sampleRecord
                    json_schema = schema_of_json(lit(sample_json))
                    output_df = in0.withColumn("json_parsed_content", from_json(column_to_parse, json_schema))
                else:
                    combined_json_df = in0\
                                           .limit(schemaInferCount)\
                                           .select(concat_ws(",", collect_list(column_to_parse)).alias("combined_json"))
                    sample_json = "[" + combined_json_df.collect()[0]["combined_json"] + "]"
                    json_schema = schema_of_json(lit(sample_json))
                    output_df = in0.withColumn(
                        "json_parsed_content",
                        from_json(column_to_parse, json_schema).getItem(0)
                    )
            else:
                try:
                    print("here")
                    json_schema = StructType.fromDDL(schema)
                    output_df = in0.withColumn("json_parsed_content", from_json(column_to_parse, json_schema))
                except :
                    print("there")
                    json_schema = schema
                    output_df = in0.withColumn(
                        "json_parsed_content",
                        expr(f"from_json({column_to_parse}, '{json_schema}')")
                    )

            return output_df
        except Exception as e:
            print(f"An error occurred while fetching data: {e}")
            raise e

    out0 = json_parse_new(
        in0,
        "json_string",
        "parseFromSampleRecord13",
        """{"id":1,"name":"Alice","age123":30}""",
        """STRUCT<
  age: LONG,
  orderid: INT,
  id: INT,
  address: STRUCT<
    city: STRING,
    state: STRING,
    zipcode: INT
  >
>""",
        2
    )

    return out0
