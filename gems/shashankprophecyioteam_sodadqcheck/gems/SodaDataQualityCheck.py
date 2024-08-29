from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import * 
import dataclasses



@dataclass(frozen=True)
class DatasetAlias:
    id: str
    alias: str
    tempViewName: str


class SodaDataQualityCheck(ComponentSpec):
    name: str = "SodaDataQualityCheck"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class SodaDataQualityCheckProperties(ComponentProperties):
        # properties for the component with default values
        sodaHost: SecretValue = field(default_factory=list)
        sodaAPIKey: SecretValue = field(default_factory=list)
        sodaSecretKey: SecretValue = field(default_factory=list)
        connStringFilePath: Optional[str] = None
        connStringUI: SecretValue = field(default_factory=list)
        scanDefinitioName: Optional[str] = None
        checks: str = ""
        activeTab: str = "soda_details"
        sodaDatasourceName: Optional[str] = None
        credsProvider: str = "sodaCredentials"
        ymlProvider: str = "ymlGem"
        ymlFilePath: Optional[str] = None
        failedChecksFilePath: str = ""
        datasetAliases: Optional[List[DatasetAlias]] = field(
            default_factory=lambda: [DatasetAlias("in0", "in0", "in0")])
        # datasetAliases: Optional[List[DatasetAlias]] = field(default_factory=list)

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component

        checkRadioGroup = (RadioGroup("")
                           .addOption("Soda Checks YML from file path", "ymlFile",
                                      description=("Provide a file path where soda checks are defined in a yml file"))
                           .addOption("Soda Checks YML in gem", "ymlGem",
                                      description="Provide the checks in yml form inside the gem"
                                      )
                           .setOptionType("button")
                           .setVariant("medium")
                           .setButtonStyle("solid")
                           .bindProperty("ymlProvider")
                           )

        credRadioGroup = RadioGroup("Soda Credentials") \
            .addOption("Soda Credentials", "sodaCredentials", description=(
            "Provide soda cloud connection details such as host, api_key_id and api_key_secret")) \
            .addOption("Soda connection string from file path", "connStringFilePath", description=(
            "Provide a file path where soda cloud and soda datasources details is defined in yml format")) \
            .addOption("Soda connection string on UI", "connStringUI",
                       description=("Provide connection string in yml format for soda cloud and soda datasources")) \
            .bindProperty("credsProvider")

        sodaCredentials = (Condition()
                           .ifEqual(PropExpr("component.properties.credsProvider"),
                                    StringExpr("sodaCredentials"))
                           .then((ColumnsLayout(gap="1rem")
                                  .addColumn(
            SecretBox("Host").bindPlaceholder("cloud.soda.io").bindProperty("sodaHost"))
                                  .addColumn(
            SecretBox("API Key").bindPlaceholder("api-key").bindProperty(
                "sodaAPIKey"))
                                  .addColumn(SecretBox("Secret Key").bindPlaceholder(
            "secret-key").bindProperty("sodaSecretKey")))))

        connStringFilePath = (Condition()
                              .ifEqual(PropExpr("component.properties.credsProvider"),
                                       StringExpr("connStringFilePath"))
                              .then(TextBox("").bindPlaceholder("YML file path of soda connection and data source details").bindProperty("connStringFilePath")))

        connStringUI = (Condition()
                        .ifEqual(PropExpr("component.properties.credsProvider"),
                                 StringExpr("connStringUI"))
                        .then(SecretBox("Soda connection details and data source details in YML format").bindPlaceholder(
            "yml config for soda cloud and soda data sources").bindProperty("connStringUI")))

        sodaDetails = ColumnsLayout(gap="1rem", height="100%") \
            .addColumn(StackLayout(direction=("vertical"), gap=("1rem"))
                       .addElement(credRadioGroup)
                       .addElement(sodaCredentials)
                       .addElement(connStringUI)
                       .addElement(connStringFilePath)
                       .addElement(TitleElement(title="File path to persist failed rows (Needed if failed row checks are added in YML)"))
                       .addElement(TextBox("").bindPlaceholder("dbfs:/FileStore/soda/failed_checks").bindProperty("failedChecksFilePath"))
                       ,
                       "2fr"
                       )

        ymlEditorDialog = StackLayout(gap="1rem", height="100%").addElement(
            TitleElement(title="Paste your checks in YML format here")).addElement(
            Editor(height=("50bh"), language="yml").withSchemaSuggestions().bindProperty("checks"))

        checks = StackLayout(gap="1rem", height="100%") \
            .addElement(TitleElement(title="Spark Dataset Details")) \
            .addElement(
            StackLayout(gap="1rem", height="25%").addElement(
                BasicTable(
                    "Test",
                    height="40bh",
                    columns=[
                        Column("Input Alias", "alias", width="30%"),
                        Column("Dataset name to be used in Soda checks", "tempViewName",
                               (TextBox("").bindPlaceholder("customer_df")), width="70%")
                    ],
                    delete=False,
                    appendNewRow=False,
                    targetColumnKey="alias",
                ).bindProperty("datasetAliases"),
            )) \
            .addElement(
            TextBox(
                "Soda Datasource Name (Optional)").bindPlaceholder(
                "gold_agg_datasets").bindProperty("sodaDatasourceName")
        ) \
            .addElement(TitleElement(title="Soda Checks")) \
            .addElement(checkRadioGroup) \
            .addElement(
            TextBox(
                "Soda scan definition name").bindPlaceholder(
                "Base data quality checks").bindProperty("scanDefinitioName")
        ) \
            .addElement(Condition()
                        .ifEqual(PropExpr("component.properties.ymlProvider"),
                                 StringExpr("ymlFile"))
                        .then(TextBox("YML file path for checks").bindPlaceholder("YML file path for checks").bindProperty("ymlFilePath"))
                        .otherwise(ymlEditorDialog))

        tabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(
            TabPane("SODA Details", "soda_details").addElement(sodaDetails)
        ) \
            .addTabPane(
            TabPane("Checks", "checks").addElement(checks)
        )

        return Dialog("SodaDataQualityCheck") \
            .addElement(ColumnsLayout(gap="1rem", height="100%") \
                        .addColumn(Ports().editableInput(True), "content")
                        .addColumn(tabs))

    def validate(self, context: WorkflowContext, component: Component[SodaDataQualityCheckProperties]) -> List[
        Diagnostic]:
        diagnostics = []

        if component.properties.credsProvider == "sodaCredentials" and not component.properties.sodaHost.parts:
            diagnostics.append(Diagnostic("properties.sodaHost", "SODA Host cannot be empty [Location]",
                                          SeverityLevelEnum.Error))

        if component.properties.credsProvider == "sodaCredentials" and not component.properties.sodaAPIKey.parts:
            diagnostics.append(Diagnostic("properties.sodaAPIKey", "SODA API Key cannot be empty [Location]",
                                          SeverityLevelEnum.Error))

        if component.properties.credsProvider == "sodaCredentials" and not component.properties.sodaSecretKey.parts:
            diagnostics.append(Diagnostic("properties.sodaSecretKey", "SODA Secret Key cannot be empty [Location]",
                                          SeverityLevelEnum.Error))

        # if component.properties.credsProvider == "connStringUI" and (component.properties.connStringUI is None or component.properties.connStringUI == ""):
        #     diagnostics.append(Diagnostic("properties.connStringUI", "YML config for soda cloud and soda data sources can not be empty",
        #                                   SeverityLevelEnum.Error))

        if component.properties.credsProvider == "connStringFilePath" and (component.properties.connStringFilePath is None or component.properties.connStringFilePath == ""):
            diagnostics.append(Diagnostic("properties.connStringFilePath", "YML file path for soda cloud and soda data sources can not be empty",
                                          SeverityLevelEnum.Error))

        if component.properties.ymlProvider == "ymlGem" and (component.properties.checks is None or component.properties.checks == ""):
            diagnostics.append(Diagnostic("properties.checks", "YML formatted checks can not be empty",
                                          SeverityLevelEnum.Error))

        if component.properties.ymlProvider == "ymlFile" and (component.properties.ymlFilePath is None or component.properties.ymlFilePath == ""):
            diagnostics.append(Diagnostic("properties.ymlFilePath", "YML formatted checks can not be empty",
                                          SeverityLevelEnum.Error))

        # Validate the component"s state
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[SodaDataQualityCheckProperties],
                 newState: Component[SodaDataQualityCheckProperties]) -> Component[
        SodaDataQualityCheckProperties]:
        old_aliases = newState.properties.datasetAliases
        new_ports = newState.ports.inputs
        new_aliases = []

        for port in new_ports:
            matching_alias = [curr_alias for curr_alias in old_aliases if
                              (curr_alias.id == port.id) or (port.slug == curr_alias.id)]
            new_alias = DatasetAlias(port.id, port.slug, port.slug)
            if len(matching_alias) != 0:
                new_aliases.append(matching_alias[0])
            else:
                new_aliases.append(new_alias)

        updatedPorts = dataclasses.replace(newState.ports, inputs=newState.ports.inputs, isCustomOutputSchema=True,
                                           autoUpdateOnRun=True)
        updatedProperties = dataclasses.replace(newState.properties, datasetAliases=new_aliases)
        return dataclasses.replace(newState, properties=updatedProperties, ports=updatedPorts)

    class SodaDataQualityCheckCode(ComponentCode):
        def __init__(self, newProps):
            self.props: SodaDataQualityCheck.SodaDataQualityCheckProperties = newProps

        def apply(self, spark: SparkSession, *inDFs: DataFrame) -> DataFrame:
            from soda.scan import Scan
            import os

            scan: SubstituteDisabled = Scan()
            if self.props.failedChecksFilePath is not None and self.props.failedChecksFilePath != "":
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
                        struct_fields = [StructField(x.get('name'), self.map_soda_type_to_spark_type(x.get('type')), True) for x in schema]
                        struct_type = StructType(struct_fields)
                        check_name = sample_context.check_name.split(" ")[0]
                        filename = check_name + ".csv"
                        file_path = os.path.join(self.base_path, filename)

                        df = self.spark.createDataFrame(rows, schema=struct_type)
                        df.write.option("header", True).mode("overwrite").csv(file_path)
                scan.sampler = CustomSampler(spark, self.props.failedChecksFilePath)
            scan.set_scan_definition_name(self.props.scanDefinitioName)

            if self.props.sodaDatasourceName is not None:
                datasets = self.props.datasetAliases
                index = 0
                for tempInDF in inDFs:
                    tempInDF.createOrReplaceTempView(datasets[index].tempViewName)
                    index = index + 1
                scan.set_data_source_name(self.props.sodaDatasourceName)
                scan.add_spark_session(spark, data_source_name=self.props.sodaDatasourceName)

            if self.props.ymlProvider != "ymlGem":
                file_path = self.props.ymlFilePath

                if file_path.startswith("dbfs:"):
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(SparkSession.builder.getOrCreate())
                    content = dbutils.fs.head(file_path)

                elif file_path.startswith("hdfs:"):
                    import subprocess
                    hdfs_path = file_path.replace("hdfs:/", "hdfs://")
                    process = subprocess.Popen(["hadoop", "fs", "-cat", hdfs_path], stdout=subprocess.PIPE)
                    (out, err) = process.communicate()
                    content = out.decode("utf-8")

                elif file_path.startswith("s3:") or file_path.startswith("s3a:"):
                    import boto3

                    s3 = boto3.client("s3")
                    bucket_name = file_path.split("/")[2]
                    key = "/".join(file_path.split("/")[3:])

                    obj = s3.get_object(Bucket=bucket_name, Key=key)
                    content = obj["Body"].read().decode("utf-8")

                else:
                    with open(file_path.replace("dbfs:/", "/dbfs/"), "r", encoding="utf-8") as f:
                        content: SubstituteDisabled = f.read()

                scan.add_sodacl_yaml_str(content)
            else:
                import yaml
                data = yaml.safe_load(self.props.checks)
                yaml_pretty = yaml.dump(data, sort_keys=False, default_flow_style=False, indent=2)
                scan.add_sodacl_yaml_str("\n" + self.props.checks)

            if self.props.credsProvider == "connStringFilePath":
                creds_file_path = self.props.connStringFilePath
                if creds_file_path.startswith("dbfs"):
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(SparkSession.builder.getOrCreate())
                    soda_creds = dbutils.fs.head(creds_file_path)
                    print("Soda Creds -> ", soda_creds)

                elif creds_file_path.startswith("hdfs"):
                    import subprocess
                    hdfs_path = creds_file_path.replace("hdfs:/", "hdfs://")
                    process = subprocess.Popen(["hadoop", "fs", "-cat", hdfs_path], stdout=subprocess.PIPE)
                    (out, err) = process.communicate()
                    soda_creds = out.decode("utf-8")

                elif creds_file_path.startswith("s3") or creds_file_path.startswith("s3a"):
                    import boto3

                    s3 = boto3.client("s3")
                    bucket_name = creds_file_path.split("/")[2]
                    key = "/".join(file_path.split("/")[3:])

                    obj = s3.get_object(Bucket=bucket_name, Key=key)
                    soda_creds = obj["Body"].read().decode("utf-8")

                else:
                    with open(creds_file_path.replace("dbfs:/", "/dbfs/"), "r", encoding="utf-8") as f:
                        soda_creds: SubstituteDisabled = f.read()
            elif self.props.credsProvider == "connStringUI":
                soda_creds = self.props.connStringUI
                print(soda_creds)
            else:
                soda_creds = f"""
                soda_cloud:
                    host: {self.props.sodaHost}
                    api_key_id: {self.props.sodaAPIKey}
                    api_key_secret: {self.props.sodaSecretKey}
                """
            scan.add_configuration_yaml_str(soda_creds)
            scan.execute()
            scan_results = scan.scan_results
            checksList = scan_results.get("checks")
            checksData = []
            for checks in checksList:
                checksData.append((
                    checks.get("name"), checks.get("type"), checks.get("dataSource"), checks.get("table"),
                    checks.get("column"), checks.get("outcome")))

            check_result_schema = StructType([StructField("check_name", StringType(), True),
                                              StructField("check_type", StringType(), True),
                                              StructField("data_source", StringType(), True),
                                              StructField("table_name", StringType(), True),
                                              StructField("column_name", StringType(), True),
                                              StructField("result", StringType(), True)])

            check_result_df = spark.createDataFrame(checksData, schema=check_result_schema)
            return check_result_df