package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}
import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.scalagems.migrations.Jdbc_QuerySecretMigration

object JDBC_QUERY extends DatasetSpec {

  val name: String = "JDBC_QUERY"
  val datasetType: String = "File"

  type PropertiesType = SQLStatementProperties
  case class SQLStatementProperties(
      @Property("Schema")
      schema: Option[StructType] = Some(StructType(Array(StructField("success", BooleanType, true)))),
      @Property("Description")
      description: Option[String] = Some(""),
      @Property("driver")
      driver: String = "org.postgresql.Driver",
      @Property("Credential Type")
      credType: String = "userPwd",
      @deprecated("This field is deprecated", "0.0.4")
      @Property("Credential Scope")
      credentialScope: Option[String] = None,
      @deprecated("This field is deprecated", "0.0.4")
      @Property("Username")
      textUsername: Option[String] = None,
      @deprecated("This field is deprecated", "0.0.4")
      @Property("Password")
      textPassword: Option[String] = None,
      @Property("Username")
      secretUsername: Option[SecretValue] = None,
      @Property("Password")
      secretPassword: Option[SecretValue] = None,
      @Property("jdbcUrl", "")
      jdbcUrl: String = "",
      @Property("Input Port Names", "")
      sql: String = "DELETE FROM customers WHERE customer_id > 0")
      extends DatasetProperties

  implicit val SQLStatementPropertiesFormat: Format[SQLStatementProperties] =
    Jsonx.formatCaseClass[SQLStatementProperties]
  @Property("Header Wrapper")
  case class HeaderValue(@Property("Header") header: String)

  def sourceDialog: DatasetDialog = DatasetDialog("csv")
    .addSection(
      "LOCATION",
      ScrollBox()
        .addElement(
          StackLayout(direction = Some("vertical"), gap = Some("1rem"))
            .addElement(
              StackLayout()
                .addElement(ColumnsLayout(gap = Some("1rem"))
                  .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                  .addColumn(
                    SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty("secretPassword"))))
            .addElement(TitleElement(title = "URL"))
            .addElement(TextBox("Driver").bindPlaceholder("org.postgresql.Driver").bindProperty("driver"))
            .addElement(TextBox("JDBC URL")
              .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
              .bindProperty("jdbcUrl"))))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout()
                .addElement(StackItem(grow = Some(1)).addElement(FieldPicker(height = Some("100%"))
                  .addField(
                    TextArea("Description", 2, placeholder = "Dataset description...").withCopilot(CopilotSpec(
                      method = "copilot/describe",
                      methodType = Some("CopilotDescribeDataSourceRequest"),
                      copilotProps = CopilotButtonTypeProps(buttonLabel = "Auto description", Align.end, gap = 4))),
                    "description",
                    true)))),
          "auto")
        .addColumn(
          StackLayout()
            .addElement(TitleElement(title = "Query"))
            .addElement(Editor(height = Some("60bh"))
              .withSchemaSuggestions()
              .bindProperty("sql"))))
    .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))

  def targetDialog: DatasetDialog = DatasetDialog("csv")

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    if (component.properties.secretUsername.forall(_.parts.isEmpty)) {
      diagnostics += Diagnostic(
        "properties.secretUsername",
        "Username cannot be empty",
        SeverityLevel.Error
      )
    }
    if (component.properties.secretPassword.forall(_.parts.isEmpty)) {
      diagnostics += Diagnostic(
        "properties.secretPassword",
        "Password cannot be empty",
        SeverityLevel.Error
      )
    }

    if (component.properties.secretPassword.exists(_.parts.exists {
      case x@TextSecret(v) if v.nonEmpty => true
      case _ => false
    })) diagnostics += Diagnostic(
      "properties.secretPassword",
      "Storing plain-text passwords poses a security risk and is not recommended. Please see https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets for suggested alternatives",
      SeverityLevel.Error
    )

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class SQLCode(props: SQLStatementProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import java.sql._
      var connection: Connection = null
      try {
        val username: String = if (props.secretUsername.isEmpty) "" else props.secretUsername.get
        val password: String = if (props.secretPassword.isEmpty) "" else props.secretPassword.get
        DriverManager.getConnection(props.jdbcUrl, username, password)

        val statement = connection.prepareStatement(props.sql)
        try statement.executeUpdate()
        finally statement.close()
      } finally if (connection != null) connection.close()
      spark.range(1).select(lit(true).as("result"))
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {}
  }

  registerPropertyEvolution(Jdbc_QuerySecretMigration)
}
