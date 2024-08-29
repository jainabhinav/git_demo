from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from soda_dq.config.ConfigStore import *
from soda_dq.functions import *

def soda_data_quality_checks(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.dbutils import DBUtils
    from soda.scan import Scan
    import os
    scan = Scan()
    scan.set_scan_definition_name("Data Quality Checks")
    in0.createOrReplaceTempView("customers_df")
    scan.set_data_source_name("soda_dq_check_demo1")
    scan.add_spark_session(spark, data_source_name = "soda_dq_check_demo1")
    from pyspark.dbutils import DBUtils
    scan.add_sodacl_yaml_str(
        DBUtils(SparkSession.builder.getOrCreate()).fs.head(
          "dbfs:/FileStore/data_engg/shashank/soda_details/soda_checks.yml"
        )
    )
    print("{}".format(DBUtils(spark).secrets.get(scope = "soda-creds-scope", key = "soda_creds")))
    scan.add_configuration_yaml_str(
        "{}".format(DBUtils(spark).secrets.get(scope = "soda-creds-scope", key = "soda_creds"))
    )
    scan.execute()
    checksData = []

    for checks in scan.scan_results.get("checks"):
        checksData.append(
            (checks.get("name"), checks.get("type"), checks.get("dataSource"), checks.get("table"),
             checks.get("column"), checks.get("outcome"))
        )

    return spark.createDataFrame(
        checksData,
        schema = StructType([
          StructField("check_name", StringType(), True),
                                                        StructField("check_type", StringType(), True),
                                                        StructField("data_source", StringType(), True),
                                                        StructField("table_name", StringType(), True),
                                                        StructField("column_name", StringType(), True),
                                                        StructField("result", StringType(), True)
      ])
    )
