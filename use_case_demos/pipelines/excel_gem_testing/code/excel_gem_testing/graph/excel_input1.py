from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_gem_testing.config.ConfigStore import *
from excel_gem_testing.functions import *

def excel_input1(spark: SparkSession) -> DataFrame:
    import pandas as pd
    kwargs = {}
    targetPath = "hkj"

    if "hkj"[:5] == "dbfs:":
        targetPath = "hkj"

    pandasDf = pd.read_excel(targetPath, **kwargs)

    return spark.createDataFrame(pandasDf)
