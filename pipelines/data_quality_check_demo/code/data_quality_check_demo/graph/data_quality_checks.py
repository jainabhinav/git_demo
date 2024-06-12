from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_quality_check_demo.config.ConfigStore import *
from data_quality_check_demo.functions import *

def data_quality_checks(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):

    @udf(returnType = StructType([StructField("check_type", StringType()), StructField("column", StringType())]))
    def extract_check_and_column(constraint_str):
        extended_check_names_with_suffix = {
            "is less than or equal to": "Column To Constant Value Less Than Or Equal To Check",
            "is less than": "Column To Constant Value Less Than Check",
            "is greater than or equal to": "Column To Constant Value Greater Than Or Equal To Check",
            "is greater than": "Column To Constant Value Greater Than Check"
        }
        extended_check_names_without_suffix = {
            "is less than or equal to": "Column To Column Less Than Or Equal To Check",
            "is less than": "Column To Column Less Than Check",
            "is greater than or equal to": "Column To Column Greater Than Or Equal To Check",
            "is greater than": "Column To Column Greater Than Check"
        }

        if re.compile(r"(\w+Constraint)\((?:\w+)?\((?:List\()?([^\),]*)").search(constraint_str):
            column_name = re\
                              .compile(r"(\w+Constraint)\((?:\w+)?\((?:List\()?([^\),]*)")\
                              .search(constraint_str)\
                              .group(2)\
                              .strip()
            check_name = {
                "CompletenessConstraint": "Completeness Check",
                "SizeConstraint": "Row Count Check",
                "UniquenessConstraint": "Uniqueness Check",
                "HistogramBinConstraint": "Distinct Count Check",
                "AnalysisBasedConstraint": "Data Type Check",
                "MaxLengthConstraint": "Min-Max Length Check",
                "SumConstraint": "Total Sum Check",
                "MeanConstraint": "Mean Value Check",
                "StandardDeviationConstraint": "Standard Deviation Check",
                "ComplianceConstraint": ""
            }\
                .get(
                re.compile(r"(\w+Constraint)\((?:\w+)?\((?:List\()?([^\),]*)").search(constraint_str).group(1),
                re.compile(r"(\w+Constraint)\((?:\w+)?\((?:List\()?([^\),]*)").search(constraint_str).group(1)
            )

            # Handle ComplianceConstraints separately
            if (
                re.compile(r"(\w+Constraint)\((?:\w+)?\((?:List\()?([^\),]*)").search(constraint_str).group(1)
                == "ComplianceConstraint"
            ):

                for key, value in {
                    "is non-negative": "Non Negative Value Check",
                    "is positive": "Positive Value Check",
                    "contained in": "Lookup Check",
                    "is greater than or equal to": "Greater Than Or Equal To Check",
                    "is greater than": "Greater Than Check",
                    "is less than or equal to": "Less Than Or Equal To Check",
                    "is less than": "Less Than Check"
                }\
                    .items(
                ):
                    if key in column_name:
                        check_name = value
                        column_name = re.sub(rf"\s*{re.escape(key)}.*", "", column_name).strip()
                        break

                # Handle extended compliance constraints with special suffixes
                for key, suffix in {
                    "is less than or equal to": "_lte_const",
                    "is less than": "_lt_const",
                    "is greater than or equal to": "_gte_const",
                    "is greater than": "_gt_const"
                }\
                    .items(
                ):
                    if key in constraint_str:

                        if suffix in constraint_str:
                            check_name = extended_check_names_with_suffix[key]
                        else:
                            check_name = extended_check_names_without_suffix[key]

                        column_name = re.sub(rf"\s*{re.escape(key)}.*{re.escape(suffix)}?", "", column_name).strip()
                        break

            if column_name == "None":
                column_name = ""

            return check_name, column_name
        else:
            return constraint_str, None

    from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
    from pydeequ.verification import VerificationSuite, VerificationResult

    return (in0,
            VerificationResult\
              .checkResultsAsDataFrame(
                spark,
                VerificationSuite(spark)\
                  .onData(in0)\
                  .addCheck(
                    Check(spark, CheckLevel.Warning, "Data Quality Checks")\
                      .hasCompleteness(
                        "customer_id",
                        lambda x: x >= 1.0,
                        hint = f"{1.0 * 100}% values should be non-null for customer_id"
                      )\
                      .hasCompleteness(
                        "first_name",
                        lambda x: x >= 1.0,
                        hint = f"{1.0 * 100}% values should be non-null for first_name"
                      )\
                      .hasCompleteness("last_name", lambda x: x >= 1.0, hint = f"{1.0 * 100}% values should be non-null for last_name")\
                      .hasSize(lambda x: x >= 10, hint = "The number of rows should be at least 10")\
                      .hasNumberOfDistinctValues(
                        "account_flags",
                        lambda x: x == 4,
                        None,
                        None,
                        hint = "Column account_flags should have 4 distinct values"
                      )\
                      .isUnique("customer_id", hint = "Column customer_id is not unique")\
                      .hasMaxLength(
                        "first_name",
                        lambda x: x >= 2 and x <= 50,
                        hint = "Length of column first_name does not lie between 2 and 50"
                      )\
                      .hasMaxLength(
                        "last_name",
                        lambda x: x >= 2 and x <= 50,
                        hint = "Length of column last_name does not lie between 2 and 50"
                      )\
                      .hasDataType(
                        "phone",
                        ConstrainableDataTypes.String,
                        lambda x: x == True,
                        hint = "Column phone is not of String data type"
                      )\
                      .isContainedIn("country_code", ["CN", "MX", "ID"], lambda x: x >= 1.0, hint = "Column country_code should have (atleast) {}% values lie in {}".format(1.0 * 100, ["CN", "MX", "ID"]))
                  )\
                  .run()
              )\
              .withColumn("parsed", extract_check_and_column(col("constraint")))\
              .select("parsed.check_type", "parsed.column", "constraint_status", "constraint_message"))
