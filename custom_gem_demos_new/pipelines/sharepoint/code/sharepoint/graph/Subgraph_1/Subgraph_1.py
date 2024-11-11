from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from sharepoint.functions import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, subgraph_config: SubgraphConfig) -> None:
    Config.update(subgraph_config)
    print_username(spark)
    subgraph_config.update(Config)
