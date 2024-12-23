from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *

def reformat_zip_to_state(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        expr(
            "ai_query('databricks-meta-llama-3-1-70b-instruct', '\"Can you tell me the name of the US state that serves the provided ZIP code 27549?\"')"
          )\
          .alias("state_name")
    )
