from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from json_column_parser.config.ConfigStore import *
from json_column_parser.functions import *
from prophecy.utils import *
from json_column_parser.graph import *

def pipeline(spark: SparkSession) -> None:
    df_create_json_string_column = create_json_string_column(spark)
    df_json_parsing_with_schema = json_parsing_with_schema(spark, df_create_json_string_column)
    df_reformat_json_parsed_content = reformat_json_parsed_content(spark, df_json_parsing_with_schema)
    df_json_column_parsing = json_column_parsing(spark, df_create_json_string_column)
    df_reformat_zip_to_state = reformat_zip_to_state(spark, df_create_json_string_column)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("json_column_parser")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/json_column_parser")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/json_column_parser", config = Config)(pipeline)

if __name__ == "__main__":
    main()
