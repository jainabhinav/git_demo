from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from rest_api_demo.config.ConfigStore import *
from rest_api_demo.functions import *

def disable_pipeline_start(spark: SparkSession):
    Config.pipeline_start = False

    return 
