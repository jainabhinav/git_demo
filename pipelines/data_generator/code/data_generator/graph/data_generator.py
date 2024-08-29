from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_generator.config.ConfigStore import *
from data_generator.functions import *

def data_generator(spark: SparkSession) -> DataFrame:
    from prophecy.utils import synthetic_data_generator
    import json

    return synthetic_data_generator\
        .FakeDataFrame(spark, 10)\
        .addColumn("name", synthetic_data_generator.RandomFullName(), data_type = StringType(), nulls = 0)\
        .addColumn("age", synthetic_data_generator.RandomInt(min = 20, max = 30), data_type = IntegerType(), nulls = 0)\
        .addColumn("cust_email", synthetic_data_generator.RandomEmail(), data_type = StringType(), nulls = 0)\
        .addColumn(
          "cust_type",
          synthetic_data_generator.RandomListElements(elements = ["bronze", "silver", "gold"]),
          data_type = StringType(),
          nulls = int(10 * (30.0 / 100))
        )\
        .build()
