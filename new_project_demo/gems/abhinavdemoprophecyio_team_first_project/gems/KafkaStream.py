from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import (
    DatasetSpec,
    DatasetProperties,
    Component,
)
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.StringUtils import isBlank
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import dataclasses
from pyspark.sql.types import StructType


class KafkaStream(DatasetSpec):
    name: str = "KafkaStream"
    datasetType: str = "File"
    docUrl: str = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/kafka"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class KafkaStreamProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        credType: str = "userPwd"
        credentialScope: Optional[str] = None
        brokerList: Optional[str] = None
        groupId: Optional[str] = ""
        sessionTimeout: Optional[str] = "6000"
        security: Optional[str] = "NO_AUTH"
        saslMechanism: Optional[str] = "NO_AUTH"
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        kafkaTopic: Optional[str] = None
        storeOffsets: Optional[bool] = True
        offsetMetaTable: Optional[str] = ""
        messageKey: Optional[SColumn] = None
        kerberosServiceName: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return (
            DatasetDialog("KafkaStream")
                .addSection(
                "LOCATION",
                StackLayout()
                    .addElement(
                    TextBox("Bootstrap Server/Broker List")
                        .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
                        .bindProperty("brokerList")
                )
                    .addElement(TextBox("Group Id (Optional)").bindPlaceholder("group_id_1").bindProperty("groupId"))
                    .addElement(
                    TextBox("Session timeout (in ms)")
                        .bindPlaceholder("6000")
                        .bindProperty("sessionTimeout")
                )
                    .addElement(
                    SelectBox("Security Protocol")
                        .addOption("SASL_SSL", "SASL_SSL")
                        .addOption("PLAINTEXT", "PLAINTEXT")
                        .addOption("SSL", "SSL")
                        .addOption("SASL_PLAINTEXT", "SASL_PLAINTEXT")
                        .addOption("NO_AUTH", "NO_AUTH")
                        .bindProperty("security")
                )
                    .addElement(
                    StackLayout()
                        .addElement(
                        SelectBox("SASL Mechanisms")
                            .addOption("SCRAM-SHA-256", "SCRAM-SHA-256")
                            .addOption("GSSAPI", "GSSAPI")
                            .addOption("PLAIN", "PLAIN")
                            .addOption("SCRAM-SHA-512", "SCRAM-SHA-512")
                            .addOption("OAUTHBEARER", "OAUTHBEARER")
                            .addOption("NO_AUTH", "NO_AUTH")
                            .bindProperty("saslMechanism")
                    ).addElement(
                        StackLayout()
                            .addElement(
                            RadioGroup("Credentials (Optional)")
                                .addOption("Databricks Secrets", "databricksSecrets")
                                .addOption("Username & Password", "userPwd")
                                .addOption("Environment variables", "userPwdEnv")
                                .bindProperty("credType")
                        ))
                        .addElement(
                        Condition()
                            .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                            .then(Credentials("").bindProperty("credentialScope"))
                            .otherwise(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                                .addColumn(
                                TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                    "textPassword")
                            )
                        )
                    )
                )
                    .addElement(
                    TextBox("Kafka topic")
                        .bindPlaceholder("my_first_topic,my_second_topic")
                        .bindProperty("kafkaTopic")
                ).addElement(
                    Checkbox(
                        "Store offsets read per partition in delta table (would require write to metadata table. Please refer docs.)")
                        .bindProperty("storeOffsets")
                ).addElement(
                    Condition()
                        .ifEqual(PropExpr("component.properties.storeOffsets"), BooleanExpr(True))
                        .then(TextBox("Metadata Table")
                              .bindPlaceholder("db.metaTable")
                              .bindProperty("offsetMetaTable"))

                ),
            )
                .addSection(
                "PROPERTIES",
                ColumnsLayout(gap=("1rem"), height=("100%")).addColumn(
                    ScrollBox().addElement(
                        StackLayout(height=("100%"))
                            .addElement(
                            StackItem(grow=(1)).addElement(
                                FieldPicker(height=("100%"))
                                    .addField(
                                    TextArea("Description", 2,
                                             placeholder="Dataset description...").withCopilotEnabledDescribeDataSource(),
                                    "description",
                                    True
                                ).addField(TextBox("Kerberos service name for Kafka SASL").bindPlaceholder("kafka"),
                                           "kerberosServiceName")
                            )
                        )
                    ),
                    "auto"
                ).addColumn(
                    SchemaTable("").isReadOnly().bindProperty("schema"), "5fr"
                ),
            )
                .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
        )

    def targetDialog(self) -> DatasetDialog:
        return DatasetDialog("KafkaStream").addSection(
            "LOCATION",
            StackLayout()
                .addElement(
                TextBox("Bootstrap Server/Broker List")
                    .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
                    .bindProperty("brokerList")
            )
                .addElement(
                SelectBox("Security Protocol")
                    .addOption("SASL_SSL", "SASL_SSL")
                    .addOption("PLAINTEXT", "PLAINTEXT")
                    .addOption("SSL", "SSL")
                    .addOption("SASL_PLAINTEXT", "SASL_PLAINTEXT")
                    .addOption("NO_AUTH", "NO_AUTH")
                    .bindProperty("security")
            )
                .addElement(
                StackLayout()
                    .addElement(
                    SelectBox("SASL Mechanisms")
                        .addOption("SCRAM-SHA-256", "SCRAM-SHA-256")
                        .addOption("GSSAPI", "GSSAPI")
                        .addOption("PLAIN", "PLAIN")
                        .addOption("SCRAM-SHA-512", "SCRAM-SHA-512")
                        .addOption("OAUTHBEARER", "OAUTHBEARER")
                        .addOption("NO_AUTH", "NO_AUTH")
                        .bindProperty("saslMechanism")
                ).addElement(
                    StackLayout()
                        .addElement(
                        RadioGroup("Credentials (Optional)")
                            .addOption("Databricks Secrets", "databricksSecrets")
                            .addOption("Username & Password", "userPwd")
                            .addOption("Environment variables", "userPwdEnv")
                            .bindProperty("credType")
                    ))
                    .addElement(
                    Condition()
                        .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                        .then(Credentials("").bindProperty("credentialScope"))
                        .otherwise(
                        ColumnsLayout(gap=("1rem"))
                            .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                            .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty(
                                "textPassword")
                        )
                    )
                )
            )
                .addElement(
                ColumnsLayout(gap=("1rem"))
                    .addColumn(
                    TextBox("Kafka topic")
                        .bindPlaceholder("my_first_topic,my_second_topic")
                        .bindProperty("kafkaTopic")
                )
            ).addElement(
                StackLayout()
                    .addElement(
                    NativeText(
                        "Message Unique Key (Optional)"
                    )
                )
                    .addElement(
                    ExpressionBox(ignoreTitle=True)
                        .makeFieldOptional()
                        .withSchemaSuggestions()
                        .bindPlaceholders({
                        "scala": """concat(col("col1"), col("col2"))""",
                        "python": """concat(col("col1"), col("col2"))""",
                        "sql": """concat(col1, col2)"""})
                        .bindProperty("messageKey")
                )
            )
        ).addSection(
            "PROPERTIES",
            ColumnsLayout(gap=("1rem"), height=("100%")).addColumn(
                ScrollBox().addElement(
                    StackLayout(height=("100%"))
                        .addElement(
                        StackItem(grow=(1)).addElement(
                            FieldPicker(height=("100%"))
                                .addField(
                                TextArea("Description", 2,
                                         placeholder="Dataset description...").withCopilotEnabledDescribeDataSource(),
                                "description",
                                True
                            ).addField(TextBox("Kerberos service name for Kafka SASL").bindPlaceholder("kafka"),
                                       "kerberosServiceName")
                        )
                    )
                ),
                "auto"
            ).addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(KafkaStream, self).validate(context, component)

        if component.properties.brokerList is None or isBlank(component.properties.brokerList):
            diagnostics.append(
                Diagnostic("properties.brokerList", "Please provide at least 1 broker.", SeverityLevelEnum.Error))

        if component.properties.security is None or isBlank(component.properties.security):
            diagnostics.append(
                Diagnostic("properties.security", "Please choose at least one security protocol",
                           SeverityLevelEnum.Error))

        if component.properties.kafkaTopic is None or isBlank(component.properties.kafkaTopic):
            diagnostics.append(
                Diagnostic("properties.kafkaTopic", "Please provide at least one kafka topic",
                           SeverityLevelEnum.Error))

        if component.properties.saslMechanism != "NO_AUTH" and component.properties.security != "NO_AUTH":
            if component.properties.credType == "databricksSecrets":
                if isBlank(component.properties.credentialScope):
                    diagnostics.append(Diagnostic(
                        "properties.credentialScope",
                        "Credential Scope cannot be empty",
                        SeverityLevelEnum.Error))
            elif component.properties.credType == "userPwd" or component.properties.credType == "userPwdEnv":
                if isBlank(component.properties.textUsername):
                    diagnostics.append(Diagnostic("properties.textUsername", "Username cannot be empty",
                                                  SeverityLevelEnum.Error))
                elif isBlank(component.properties.textPassword):
                    diagnostics.append(Diagnostic("properties.textPassword", "Password cannot be empty [Location]",
                                                  SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class KafkaStreamFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: KafkaStream.KafkaStreamProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.format("kafka")

            if self.props.saslMechanism != "NO_AUTH" and self.props.security != "NO_AUTH":
                if self.props.credType == "databricksSecrets":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    username = dbutils.secrets.get(scope=self.props.credentialScope, key="username")
                    password = dbutils.secrets.get(scope=self.props.credentialScope, key="password")
                elif self.props.credType == "userPwdEnv":
                    import os
                    username = os.environ[self.props.textUsername]
                    password = os.environ[self.props.textPassword]
                else:
                    username = self.props.textUsername
                    password = self.props.textPassword
                jaas_config = f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule" + f' required username="{username}" password="{password}";'
                if self.props.saslMechanism == "PLAIN" and self.props.security == "SASL_PLAINTEXT":
                    jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule" + f' required username="{username}" password="{password}";'

                reader = reader.option("kafka.sasl.mechanism", self.props.saslMechanism).option(
                    "kafka.security.protocol", self.props.security).option("kafka.sasl.jaas.config", jaas_config)

            if self.props.brokerList is not None:
                reader = reader.option("kafka.bootstrap.servers", self.props.brokerList)

            if self.props.kerberosServiceName is not None:
                reader = reader.option("kafka.sasl.kerberos.service.name", self.props.kerberosServiceName)

            if self.props.groupId is not None and self.props.groupId != "":
                reader = reader.option("group.id", self.props.groupId)

            if self.props.kafkaTopic is not None:
                reader = reader.option("subscribe", self.props.kafkaTopic)

            if self.props.sessionTimeout is not None and self.props.sessionTimeout != "":
                reader = reader.option("kafka.session.timeout.ms", self.props.sessionTimeout)

            if self.props.storeOffsets:
                from pyspark.sql.utils import AnalysisException
                import json
                table_name = f"{self.props.offsetMetaTable}"
                try:
                    desc_table = spark.sql(f"describe formatted {table_name}")
                    table_exists = True
                except AnalysisException as e:
                    table_exists = False
                if table_exists:
                    from delta.tables import DeltaTable
                    deltaTable = DeltaTable.forName(
                        spark, f"{self.props.offsetMetaTable}"
                    ).toDF()

                    meta = deltaTable.collect()

                    offset_dict = {}
                    for row in meta:
                        if row["topic"] in offset_dict.keys():
                            offset_dict[row["topic"]].update(
                                {row["partition"]: row["max_offset"] + 1}
                            )
                        else:
                            offset_dict[row["topic"]] = {
                                row["partition"]: row["max_offset"] + 1
                            }

                    reader = reader.option("startingOffsets", json.dumps(offset_dict))

            df = reader \
                .load() \
                .withColumn("value", col("value").cast("string")) \
                .withColumn("key", col("key").cast("string"))
            return df

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            if self.props.messageKey is None:
                writer = in0.select(to_json(struct("*")).alias("value")).selectExpr(
                    "CAST(value AS STRING)"
                ).write.format("kafka")
            else:
                writer = in0.select(self.props.messageKey.column().alias("key"),
                                    to_json(struct("*")).alias("value")).selectExpr(
                    "CAST(key AS STRING)", "CAST(value AS STRING)"
                ).write.format("kafka")

            if self.props.saslMechanism != "NO_AUTH" and self.props.security != "NO_AUTH":
                if self.props.credType == "databricksSecrets":
                    from pyspark.dbutils import DBUtils
                    dbutils = DBUtils(spark)
                    username = dbutils.secrets.get(scope=self.props.credentialScope, key="username")
                    password = dbutils.secrets.get(scope=self.props.credentialScope, key="password")
                elif self.props.credType == "userPwdEnv":
                    import os
                    username = os.environ[self.props.textUsername]
                    password = os.environ[self.props.textPassword]
                else:
                    username = self.props.textUsername
                    password = self.props.textPassword
                jaas_config = f"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule" + f' required username="{username}" password="{password}";'
                if self.props.saslMechanism == "PLAIN" and self.props.security == "SASL_PLAINTEXT":
                    jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule" + f' required username="{username}" password="{password}";'

                writer = writer.option("kafka.sasl.mechanism", self.props.saslMechanism).option(
                    "kafka.security.protocol", self.props.security).option("kafka.sasl.jaas.config", jaas_config)

            if self.props.brokerList is not None:
                writer = writer.option("kafka.bootstrap.servers", self.props.brokerList)

            if self.props.kerberosServiceName is not None:
                writer = writer.option("kafka.sasl.kerberos.service.name", self.props.kerberosServiceName)

            if self.props.kafkaTopic is not None:
                writer = writer.option("topic", self.props.kafkaTopic)

            writer.save()
