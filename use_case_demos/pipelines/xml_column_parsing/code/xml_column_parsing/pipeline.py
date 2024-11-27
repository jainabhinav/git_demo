from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from xml_column_parsing.config.ConfigStore import *
from xml_column_parsing.functions import *
from prophecy.utils import *
from xml_column_parsing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_xml_column_parsing_source_1 = xml_column_parsing_source_1(spark)
    df_parse_schema = parse_schema(spark, df_xml_column_parsing_source_1)
    df_xml_column_parsing_source = xml_column_parsing_source(spark)
    df_xml_column_parser = xml_column_parser(spark, df_xml_column_parsing_source)
    df_flatten_schema = flatten_schema(spark, df_xml_column_parser)
    df_parse_xml_column_1 = parse_xml_column_1(spark, df_xml_column_parsing_source)
    df_xml_column_parser_1 = xml_column_parser_1(spark, df_xml_column_parsing_source)
    df_flatten_schema_2 = flatten_schema_2(spark, df_xml_column_parser_1)
    df_ColumnParser_2 = ColumnParser_2(spark, df_xml_column_parsing_source)
    df_parse_xml_column = parse_xml_column(spark, df_xml_column_parsing_source)
    df_flatten_schema_1 = flatten_schema_1(spark, df_parse_xml_column)
    df_ColumnParser_1 = ColumnParser_1(spark, df_xml_column_parsing_source)
    df_parse_auto = parse_auto(spark, df_xml_column_parsing_source_1)
    df_reformatted_person_data = reformatted_person_data(spark, df_xml_column_parser)
    df_parse_sample_record = parse_sample_record(spark, df_xml_column_parsing_source_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("xml_column_parsing")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/xml_column_parsing")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/xml_column_parsing", config = Config)(pipeline)

if __name__ == "__main__":
    main()
