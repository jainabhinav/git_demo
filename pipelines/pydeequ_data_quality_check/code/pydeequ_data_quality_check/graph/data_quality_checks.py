from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pydeequ_data_quality_check.config.ConfigStore import *
from pydeequ_data_quality_check.functions import *

def data_quality_checks(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    import re
    from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
    from pydeequ.verification import VerificationSuite, VerificationResult
    failed_checks_count = VerificationResult\
                              .checkResultsAsDataFrame(
                                spark,
                                VerificationSuite(spark)\
                                  .onData(in0)\
                                  .addCheck(
                                    Check(spark, CheckLevel.Warning, "Data Quality Checks")\
                                      .hasCompleteness("id", lambda x: x >= 1.0, hint = "There are some null values in the column")\
                                      .hasCompleteness(
                                        "EmployeeName",
                                        lambda x: x >= 1.0,
                                        hint = "There are some null values in the column"
                                      )\
                                      .hasSize(lambda x: x >= 3, hint = "Dataframe contains rows less than 3")\
                                      .isUnique("id", hint = "Column id is not unique")\
                                      .hasNumberOfDistinctValues(
                                        "Department",
                                        lambda x: x == 2,
                                        None,
                                        None,
                                        hint = "Department column should have 2 distinct values"
                                      )\
                                      .hasDataType(
                                        "EmployeeName",
                                        ConstrainableDataTypes.String,
                                        lambda x: x == True,
                                        hint = "Column EmployeeName is not of String data type"
                                      )\
                                      .hasSum("new_salary", lambda x: x == 1000.0, hint = "Sum of salary should be 1000")\
                                      .hasMaxLength("EmployeeName", lambda x: x >= 2 and x <= 4, hint = "Length of column EmployeeName does not lie between 2 and 4")
                                  )\
                                  .run()
                              )\
                              .selectExpr(
                                "constraint_status",
                                "constraint_message",
                                "udf_extract_check_and_column(constraint) as parsed"
                              )\
                              .selectExpr(
                                "parsed._1 as check_type",
                                "parsed._2 as column",
                                "constraint_status",
                                "constraint_message"
                              )\
                              .filter(col('constraint_status') != 'Success')\
                              .count()

    if failed_checks_count > 1:
        print(f"Data quality check failed: {failed_checks_count} checks did not pass.")
        raise Exception(f"Data quality check failed: {failed_checks_count} checks did not pass.")

    return (in0,
            VerificationResult\
              .checkResultsAsDataFrame(
                spark,
                VerificationSuite(spark)\
                  .onData(in0)\
                  .addCheck(
                    Check(spark, CheckLevel.Warning, "Data Quality Checks")\
                      .hasCompleteness("id", lambda x: x >= 1.0, hint = "There are some null values in the column")\
                      .hasCompleteness("EmployeeName", lambda x: x >= 1.0, hint = "There are some null values in the column")\
                      .hasSize(lambda x: x >= 3, hint = "Dataframe contains rows less than 3")\
                      .isUnique("id", hint = "Column id is not unique")\
                      .hasNumberOfDistinctValues(
                        "Department",
                        lambda x: x == 2,
                        None,
                        None,
                        hint = "Department column should have 2 distinct values"
                      )\
                      .hasDataType(
                        "EmployeeName",
                        ConstrainableDataTypes.String,
                        lambda x: x == True,
                        hint = "Column EmployeeName is not of String data type"
                      )\
                      .hasSum("new_salary", lambda x: x == 1000.0, hint = "Sum of salary should be 1000")\
                      .hasMaxLength("EmployeeName", lambda x: x >= 2 and x <= 4, hint = "Length of column EmployeeName does not lie between 2 and 4")
                  )\
                  .run()
              )\
              .selectExpr("constraint_status", "constraint_message", "udf_extract_check_and_column(constraint) as parsed")\
              .selectExpr("parsed._1 as check_type", "parsed._2 as column", "constraint_status", "constraint_message"))
