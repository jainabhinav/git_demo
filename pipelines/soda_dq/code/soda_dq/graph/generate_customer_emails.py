from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from soda_dq.config.ConfigStore import *
from soda_dq.functions import *

def generate_customer_emails(spark: SparkSession) -> DataFrame:
    from pyspark.sql.functions import col, concat, lit, when
    from pyspark.sql.types import IntegerType
    import random
    # Generate 9 records with sequential customer_id, email, and age
    data = [(i, random.randint(18, 70)) for i in range(2, 11)]
    # Add the specific record with customer_id 1 and the email "celes1@usa.gov"
    data.insert(0, (1, random.randint(18, 70)))
    # Create DataFrame
    df = spark.createDataFrame(data, ["customer_id", "age"])
    # Add email column, ensuring "celes1@usa.gov" for customer_id 1
    out0 = df.withColumn(
        "email",
        when(col("customer_id") == 1, lit("celes1@usa.gov"))\
          .otherwise(concat(col("customer_id").cast(StringType()), lit("@example.com")))
    )

    return out0
