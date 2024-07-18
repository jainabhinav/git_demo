from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from iceberg_demo.config.ConfigStore import *
from iceberg_demo.functions import *

def sample_data(spark: SparkSession) -> DataFrame:
    from py4j.java_gateway import java_import
    java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
    data = [(1, "James", "Sales", 3000, 3500), (2, "Michael", "Sales", 4600, 5100), (3, "Shashank", "Hr", 4000, 4500),
            (4, "Amit", "Finance", 3000, 3500), (5, "Nitin", "Finance", 5000, 5500)]
    columns = ["id", "EmployeeName", "Department", "old_salary", "new_salary"]
    out0 = spark.createDataFrame(data, schema = columns)

    return out0

    return out0
