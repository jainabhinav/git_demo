from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from code_regeneration_demo.config.ConfigStore import *
from code_regeneration_demo.functions import *

def csv_source_test2(spark: SparkSession) -> DataFrame:
    from pyspark.dbutils import DBUtils
    import os
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.user_credential import UserCredential

    with open(os.path.join("/tmp", os.path.basename("xcvxcv")), "wb") as local_file:
        ClientContext("asd")\
            .with_credentials(UserCredential(
            "asd",
            "{}".format(DBUtils(spark).secrets.get(scope = "adhoc_prophecy_jobs", key = "tetsets"))
        ))\
            .web\
            .get_file_by_server_relative_url("xcvxcv")\
            .download(local_file)\
            .execute_query()

    with open(os.path.join("/tmp", os.path.basename("xcvxcv")), 'r') as f:
        lines = f.readlines()

    rdd = spark.sparkContext.parallelize(lines)

    return spark.read\
        .schema(StructType([StructField("a", StringType(), True), StructField("c", StringType(), True)]))\
        .option("header", True)\
        .option("sep", ",")\
        .csv(rdd)
