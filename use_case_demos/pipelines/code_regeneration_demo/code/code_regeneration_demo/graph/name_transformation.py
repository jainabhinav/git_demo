from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *

def name_transformation(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumns(
        {
          "full_name": concat(col("first_name"), lit(" "), col("last_name")), 
          "upper_case_name": upper(col("full_name"))
        }
    )
