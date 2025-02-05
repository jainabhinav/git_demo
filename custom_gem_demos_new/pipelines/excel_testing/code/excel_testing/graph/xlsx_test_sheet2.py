from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *

def xlsx_test_sheet2(spark: SparkSession) -> DataFrame:
    import pandas as pd
    kwargs = {}
    kwargs["sheet_name"] = "sheet1"
    targetPath = "dbfs:/Prophecy/abhinav/test_excel/test1.xlsx"

    if "dbfs:/Prophecy/abhinav/test_excel/test1.xlsx"[:5] == "dbfs:":
        targetPath = "/dbfs/Prophecy/abhinav/test_excel/test1.xlsx"

    pandasDf = pd.read_excel(targetPath, **kwargs)

    return spark.createDataFrame(pandasDf)
