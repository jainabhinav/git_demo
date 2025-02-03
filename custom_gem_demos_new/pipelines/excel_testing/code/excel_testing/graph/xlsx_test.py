from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *

def xlsx_test(spark: SparkSession) -> DataFrame:
    import pandas as pd
    kwargs = {}
    kwargs["sheet_name"] = "sheet2"
    targetPath = "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"

    if "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"[:5] == "dbfs:":
        targetPath = "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"

    pandasDf = pd.read_excel(targetPath, **kwargs)

    return spark.createDataFrame(pandasDf)
