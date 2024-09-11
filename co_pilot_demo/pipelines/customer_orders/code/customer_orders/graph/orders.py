from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders.config.ConfigStore import *
from customer_orders.functions import *

def orders(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("order_id", IntegerType(), True), StructField("customer_id", IntegerType(), True), StructField("order_status", StringType(), True), StructField("order_category", StringType(), True), StructField("order_date", DateType(), True), StructField("amount", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/109f794f881c8194b6a2b869509675e9/OrdersDatasetInput.csv")
