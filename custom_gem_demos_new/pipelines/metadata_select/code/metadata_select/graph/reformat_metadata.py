from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from metadata_select.config.ConfigStore import *
from metadata_select.functions import *

def reformat_metadata(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    df1 = in1.select("target_column", "target_expression")
    df3 = df1.collect()
    df4 = in0.select(*[
              expr(row["target_expression"]).alias(row["target_column"])
              for row in df3
              ])
    df2 = df4

    return df2
