import dataclasses

from prophecy.cb.migration import PropertyMigrationObj
from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum, \
    SubstituteDisabled
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


class redshift(DatasetSpec):
    name: str = "redshift"
    datasetType: str = "Warehouse"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/redshift"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class RedshiftProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "databricksSecrets"  # Deprecated
        credentialScope: Optional[str] = None  # Deprecated
        textUsername: Optional[str] = None  # Deprecated
        textPassword: Optional[str] = None  # Deprecated
        jdbcUrl: str = ""  # Deprecated
        secretUsername: SecretValue = field(default_factory=list)
        secretPassword: SecretValue = field(default_factory=list)
        secretJdbcUrl: SecretValue = field(default_factory=list)
        readFromSource: str = "dbtable"
        dbtable: Optional[str] = None
        query: Optional[str] = None
        driver: Optional[str] = None
        writeMode: Optional[str] = None
        jdbcTempDir: Optional[str] = None
        forward_spark_s3_credentials: Optional[bool] = None
        aws_iam_role: Optional[str] = None
        temporary_aws_access_key_id: Optional[bool] = None
        diststyle: Optional[str] = None
        distkey: Optional[str] = None
        sqlPreAction: Optional[SColumn] = None
        sqlPostAction: Optional[SColumn] = None
        str_max_length: Optional[str] = None
        secrets_name_user: Optional[str] = None
        secrets_name_password: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return DatasetDialog("redshift") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(TitleElement(title="Credentials"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                            .addColumn(SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                            "secretPassword"))
                    )
                )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    SecretBox("JDBC URL")
                        .bindPlaceholder("jdbc:redshift://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("secretJdbcUrl")
                ).addElement(
                    TextBox("Temporary Directory").bindPlaceholder("s3a://redshift_bucket").bindProperty("jdbcTempDir"))
                    #            .addElement(TitleElement(title = "Table"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Data Source")
                            .addOption("DB Table", "dbtable")
                            .addOption("SQL Query", "sqlQuery")
                            .bindProperty("readFromSource")
                    )
                        .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                            .then(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
                            .otherwise(
                            TextBox("SQL Query").bindPlaceholder("select c1, c2 from t1").bindProperty("query")
                        )
                    )
                )
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        Checkbox("Forward S3 access credentials to databricks").bindProperty(
                            "forward_spark_s3_credentials")
                    )
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(TextBox("Driver").bindPlaceholder("com.amazon.redshift.jdbc42.Driver"),
                                          "driver")
                                .addField(TextBox("AWS IAM Role").bindPlaceholder(
                                "arn:aws:iam::123456789000:role/<redshift-iam-role>"), "aws_iam_role")
                                .addField(Checkbox("Temporary AWS access key id"), "temporary_aws_access_key_id")
                        )
                    )
                ),
                "400px"
            )
                .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
        ) \
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema"))

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("redshift") \
            .addSection(
            "LOCATION",
            ColumnsLayout()
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(TitleElement(title="Credentials"))
                    .addElement(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                            .addColumn(SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                            "secretPassword"))
                    )
                )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    SecretBox("JDBC URL")
                        .bindPlaceholder("jdbc:redshift://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                        .bindProperty("secretJdbcUrl")
                )
                    .addElement(TextBox("Temporary Directory").bindPlaceholder("s3a://redshift_bucket/").bindProperty(
                    "jdbcTempDir"))
                    .addElement(TitleElement(title="Table"))
                    .addElement(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
            )
        ) \
            .addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        Checkbox("Forward S3 access credentials to databricks").bindProperty(
                            "forward_spark_s3_credentials")
                    ).addElement(SelectBox("Write Mode")
                                 .addOption("error", "error")
                                 .addOption("overwrite", "overwrite")
                                 .addOption("append", "append")
                                 .addOption("ignore", "ignore").bindProperty("writeMode"))
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(TextBox("Driver").bindPlaceholder("org.amazon.redshift.jdbc42.Driver"),
                                          "driver")
                                .addField(TextBox("AWS IAM Role").bindPlaceholder(
                                "arn:aws:iam::123456789000:role/<redshift-iam-role>"), "aws_iam_role")
                                .addField(Checkbox("Temporary AWS access key id"), "temporary_aws_access_key_id")
                                .addField(TextBox("Max length for string columns in redshift").bindPlaceholder("2048"),
                                          "str_max_length")
                                .addField(
                                SelectBox("Row distribution style for new table")
                                    .addOption("EVEN", "EVEN")
                                    .addOption("KEY", "KEY")
                                    .addOption("ALL", "ALL")
                                , "diststyle"
                            )
                                .addField(TextBox("Distribution key for new table").bindPlaceholder(""), "distkey")
                        )
                    )
                ),
                "400px"
            )
                .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(redshift, self).validate(context, component)

        if not component.properties.secretUsername.parts:
            diagnostics.append(Diagnostic("properties.secretUsername", "Username cannot be empty [Location]",
                                          SeverityLevelEnum.Error))
        elif not component.properties.secretPassword.parts:
            diagnostics.append(Diagnostic("properties.secretPassword", "Password cannot be empty [Location]",
                                          SeverityLevelEnum.Error))

        if not component.properties.secretJdbcUrl.parts:
            diagnostics.append(
                Diagnostic("properties.secretJdbcUrl", "JDBC URL cannot be empty [Location]", SeverityLevelEnum.Error))

        if any(isinstance(x, TextSecret) and x.value for x in component.properties.secretPassword.parts):
            diagnostics.append(Diagnostic("properties.secretPassword",
                                          "Storing plain-text passwords poses a security risk and is not recommended. Please see https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets for suggested alternatives",
                                          SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class RedshiftFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: redshift.RedshiftProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("com.databricks.spark.redshift")
            reader = reader.option("url", self.props.secretJdbcUrl)
            reader = reader.option("user", self.props.secretUsername)
            reader = reader.option("password", self.props.secretPassword)

            if self.props.readFromSource == "dbtable":
                reader = reader.option("dbtable", self.props.dbtable)
            elif self.props.readFromSource == "sqlQuery":
                reader = reader.option("query", self.props.query)

            if self.props.aws_iam_role is not None:
                reader = reader.option("aws_iam_role", self.props.aws_iam_role)
            if self.props.forward_spark_s3_credentials is not None:
                reader = reader.option("forward_spark_s3_credentials", self.props.forward_spark_s3_credentials)
            if self.props.jdbcTempDir is not None:
                reader = reader.option("tempdir", self.props.jdbcTempDir)
            if self.props.driver is not None:
                reader = reader.option("jdbcdriver", self.props.driver)
            if self.props.temporary_aws_access_key_id is not None:
                reader = reader.option("temporary_aws_access_key_id", self.props.temporary_aws_access_key_id)

            return reader.load()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            df = in0
            if self.props.str_max_length is not None:
                meta_dict = {"maxlength": int(self.props.str_max_length)}
                fields = []
                old_schema: SubstituteDisabled = in0.schema
                for col in in0.columns:
                    if old_schema[col].dataType == "StringType":
                        fields.append(StructField(col, eval(old_schema[col].dataType + "()"),
                                                  metadata=meta_dict))
                    else:
                        fields.append(old_schema[col])
                new_schema: SubstituteDisabled = StructType(fields)
                df = spark.createDataFrame(in0.rdd, new_schema)

            writer = df.write.format("com.databricks.spark.redshift")

            writer = writer.option("url", self.props.secretJdbcUrl)
            writer = writer.option("user", self.props.secretUsername)
            writer = writer.option("password", self.props.secretPassword)

            if self.props.dbtable is not None:
                writer = writer.option("dbtable", self.props.dbtable)

            if self.props.aws_iam_role is not None:
                writer = writer.option("aws_iam_role", self.props.aws_iam_role)

            if self.props.forward_spark_s3_credentials is not None:
                writer = writer.option("forward_spark_s3_credentials", self.props.forward_spark_s3_credentials)
            if self.props.jdbcTempDir is not None:
                writer = writer.option("tempdir", self.props.jdbcTempDir)
            if self.props.driver is not None:
                writer = writer.option("jdbcdriver", self.props.driver)
            if self.props.temporary_aws_access_key_id is not None:
                writer = writer.option("temporary_aws_access_key_id", self.props.temporary_aws_access_key_id)
            if self.props.distkey is not None:
                writer = writer.option("distkey", self.props.distkey)
            if self.props.diststyle is not None:
                writer = writer.option("queryTimeout", self.props.diststyle)

            if self.props.writeMode is not None:
                writer = writer.mode(self.props.writeMode)

            writer.save()

    def __init__(self):
        super().__init__()
        self.registerPropertyEvolution(RedshiftPropertyMigration())


class RedshiftPropertyMigration(PropertyMigrationObj):

    def migrationNumber(self) -> int:
        return 1

    def up(self, old_properties: redshift.RedshiftProperties) -> redshift.RedshiftProperties:
        credType = old_properties.credType

        if credType == "databricksSecrets":
            new_username = SecretValue(
                [VaultSecret("Databricks", "", "0", old_properties.credentialScope, old_properties.secrets_name_user)])
            new_password = SecretValue([VaultSecret("Databricks", "", "0", old_properties.credentialScope,
                                                    old_properties.secrets_name_password)])
            new_url = SecretValue(SecretValuePart.convertTextToSecret(old_properties.jdbcUrl))
        elif credType == "userPwd":
            new_username = SecretValue(SecretValuePart.convertTextToSecret(old_properties.textUsername))
            new_password = SecretValue(SecretValuePart.convertTextToSecret(old_properties.textPassword))
            new_url = SecretValue(SecretValuePart.convertTextToSecret(old_properties.jdbcUrl))
        else:
            raise Exception("Invalid credType:" + credType)

        return dataclasses.replace(
            old_properties,
            credType="",
            credentialScope=None,
            textUsername=None,
            textPassword=None,
            jdbcUrl="",
            secrets_name_user=None,
            secrets_name_password=None,
            secretUsername=new_username,
            secretPassword=new_password,
            secretJdbcUrl=new_url
        )

    def down(self, new_properties: redshift.RedshiftProperties) -> redshift.RedshiftProperties:
        raise Exception("Downgrade is not implemented for this Redshift version")
