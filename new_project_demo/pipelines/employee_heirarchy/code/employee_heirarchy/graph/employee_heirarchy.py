from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from employee_heirarchy.config.ConfigStore import *
from employee_heirarchy.functions import *

def employee_heirarchy(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`abhinav_demo`.`employee_heirarchy`")
