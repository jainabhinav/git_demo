from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def restore_delta_table_version(spark: SparkSession):
    if not ("SColumnExpression" in locals()):
        from delta.tables import DeltaTable
        DeltaTable.forName(spark, "`abhinav_demo`.`customer_scd2_new`").restoreToVersion(0)
