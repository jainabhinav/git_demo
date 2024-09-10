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
    from soda.sampler.sampler import Sampler
    from soda.sampler.sample_context import SampleContext


    #preserveClassDef
    class CustomSampler(Sampler):

        def __init__(self, spark: SparkSession, base_path: str):
            self.spark: SparkSession = spark
            self.base_path: str = base_path

        def map_soda_type_to_spark_type(self, soda_type: str):
            if soda_type == 'STRING':
                return StringType()
            elif soda_type == 'INTEGER':
                return IntegerType()
            elif soda_type == 'FLOAT':
                return FloatType()
            elif soda_type == 'DOUBLE':
                return DoubleType()
            elif soda_type == 'BOOLEAN':
                return BooleanType()
            elif soda_type == 'DATE':
                return DateType()
            elif soda_type == 'TIMESTAMP':
                return TimestampType()
            else:
                return StringType()

        def store_sample(self, sample_context: SampleContext):
            rows = sample_context.sample.get_rows()
            schema = sample_context.sample.get_schema().get_dict()
            header = [x.get('name') for x in schema]
            struct_fields = [
                                StructField(x.get('name'), self.map_soda_type_to_spark_type(x.get('type')), True)
                                for x in schema
                                ]
            struct_type = StructType(struct_fields)
            check_name = sample_context.check_name.split(" ")[0]
            filename = check_name + ".csv"
            file_path = os.path.join(self.base_path, filename)
            df = self.spark.createDataFrame(rows, schema = struct_type)
            df.write.option("header", True).mode("overwrite").csv(file_path)

    scan.sampler = CustomSampler(spark, "dbfs:/FileStore/data_engg/shashank/soda_demo")
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
    from pyspark.dbutils import DBUtils
    print(
        "Soda Creds -> ",
        DBUtils(SparkSession.builder.getOrCreate()).fs.head(
          "dbfs:/FileStore/data_engg/shashank/soda_details/soda_creds.yml"
        )
    )
    scan.add_configuration_yaml_str(
        DBUtils(SparkSession.builder.getOrCreate()).fs.head(
          "dbfs:/FileStore/data_engg/shashank/soda_details/soda_creds.yml"
        )
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
