from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from rest_api_demo.config.ConfigStore import *
from rest_api_demo.functions import *

def rest_api_enrichment(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.udfs import get_rest_api
    requestDF = in0.withColumn(
        "api_output",
        get_rest_api(
          to_json(
            struct(lit("GET").alias("method"), expr("url").alias("url"), lit(Config.coin_api_key).alias("headers"))
          ),
          lit("")
        )
    )

    return requestDF.withColumn(
        "content_parsed",
        from_json(col("api_output.content"), schema_of_json(requestDF.select("api_output.content").take(1)[0][0]))
    )

    return requestDF
