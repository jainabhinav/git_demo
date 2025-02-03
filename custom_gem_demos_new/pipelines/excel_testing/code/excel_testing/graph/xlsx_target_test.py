from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from excel_testing.config.ConfigStore import *
from excel_testing.functions import *

def xlsx_target_test(spark: SparkSession, in0: DataFrame):
    import pandas as pd
    import os
    import shutil
    kwargs = {}
    kwargs["sheet_name"] = "sheet2"
    kwargs["header"] = True
    kwargs["index"] = False
    fileName = "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx".split("/")[- 1]
    localPath = "/tmp/" + fileName
    pandas_df = in0.toPandas()
    writePath = "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"

    if "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"[:5] == "dbfs:":
        writePath = "/Volumes/abhinav_demos/demos/test_volume/test_excel/test4.xlsx"

    if not os.path.exists(os.path.dirname(writePath)):
        os.makedirs(os.path.dirname(writePath))

    if os.path.exists(writePath):
        shutil.copy(writePath, localPath)

        with pd.ExcelWriter(localPath, engine = "openpyxl", mode = "a", if_sheet_exists = "overlay", ) as writer:
            try:
                target_sheet = (kwargs["sheet_name"] if "sheet_name" in kwargs.keys() else "Sheet1")
                existing_data_rows = pd.read_excel(
                    localPath,
                    sheet_name = (kwargs["sheet_name"] if "sheet_name" in kwargs.keys() else "Sheet1"),
                    usecols = [0]
                )\
                    .shape[0]
                kwargs["index"] = False
                kwargs["startrow"] = existing_data_rows + 1
                kwargs["header"] = False
                pandas_df.to_excel(writer, **kwargs)
            except ValueError:
                pandas_df.to_excel(writer, **kwargs)

        os.remove(writePath)
    else:
        pandas_df.to_excel(localPath, **kwargs)

    shutil.copy(localPath, writePath)

    if os.path.exists(localPath):
        os.remove(localPath)
