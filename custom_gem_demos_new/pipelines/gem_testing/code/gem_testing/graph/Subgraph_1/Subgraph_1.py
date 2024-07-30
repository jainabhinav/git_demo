from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from gem_testing.functions import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    df_select_1 = select_1(spark)
    df_Limit_1 = Limit_1(spark, df_select_1)
    subgraph_config.update(Config)
