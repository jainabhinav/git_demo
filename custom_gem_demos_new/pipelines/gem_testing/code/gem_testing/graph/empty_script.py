from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gem_testing.config.ConfigStore import *
from gem_testing.functions import *

def empty_script(spark: SparkSession):

    for char in Config.secret_test:
        print(char, end = " ")

    return 
