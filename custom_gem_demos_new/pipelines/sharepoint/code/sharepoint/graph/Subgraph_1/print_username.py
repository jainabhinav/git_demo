from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from sharepoint.functions import *

def print_username(spark: SparkSession):
    print(Config.username)

    return 
