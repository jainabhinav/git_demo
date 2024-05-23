package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, ProphecyDataFrame, SparkSession}
import play.api.libs.json.{Format, Json}
class RenameColumns extends ComponentSpec {

  val name: String = "RenameColumns"
  val category: String = "Transform"
  type PropertiesType = RenameColumnsProperties
  override def optimizeCode: Boolean = true

  case class RenameColumnsProperties(
    @Property("schema")
    schema: Option[StructType] = None,
    @Property("Rename Method")
    renameMethod: String = "",
    @Property("columnNames", "Selected Columns")
    columnNames: Option[List[String]] = None,
    @Property("prefix", "Prefix")
    prefix: Option[String] = None,
    @Property("suffix", "Suffix")
    suffix: Option[String] = None,
    @Property("customExpression", "CustomExpression")
    customExpression: Option[String] = None
  ) extends ComponentProperties

  implicit val RenameColumnsPropertiesFormat: Format[RenameColumnsProperties] = Json.format

  def dialog: Dialog = {
    val renameMethod = SelectBox("Select a method to rename columns")
      .addOption("Add Prefix", "Add Prefix")
      .addOption("Add Suffix", "Add Suffix")
      .addOption("Custom Expression", "Custom Expression")
      .bindProperty("renameMethod")

    Dialog("RenameColumns")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(Ports(), "content")
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(renameMethod)
              .addElement(
                SchemaColumnsDropdown("Select columns to rename")
                  .withMultipleSelection()
                  .bindSchema("schema")
                  .bindProperty("columnNames")
              )
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Add Prefix"))
                  .then(
                    StackLayout()
                      .addElement(
                        TextBox("Enter Prefix")
                          .bindPlaceholder("""Example: new_""")
                          .bindProperty("prefix")
                      )
                  )
              )
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Add Suffix"))
                  .then(
                    StackLayout()
                      .addElement(
                        TextBox("Enter Suffix")
                          .bindPlaceholder("""Example: _new""")
                          .bindProperty("suffix")
                      )
                  )
              )
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Custom Expression"))
                  .then(
                    StackLayout()
                      .addElement(
                        TextBox("Enter Custom Expression")
                          .bindPlaceholder("""Write spark sql expression considering `column_name` as column. Example: concat(column_name, '_suffix')""")
                          .bindProperty("customExpression")
                      )
                  )
              )
          )
      )

  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.properties.renameMethod != ""){
      if (component.properties.columnNames.isEmpty) {
        diagnostics += Diagnostic(
          "properties.columnNames",
          "Columns cannot be empty",
          SeverityLevel.Error
        )
      }
      component.properties.renameMethod match {
        case "Add Prefix" =>
          if (component.properties.prefix.isEmpty)
            diagnostics += Diagnostic(
              "properties.prefix",
              "Prefix cannot be empty",
              SeverityLevel.Error
            )
        case "Add Suffix" =>
          if (component.properties.suffix.isEmpty)
            diagnostics += Diagnostic(
              "properties.suffix",
              "Suffix cannot be empty",
              SeverityLevel.Error
            )
        case "Custom Expression" =>
          if (component.properties.customExpression.isEmpty)
            diagnostics += Diagnostic(
              "properties.customExpression",
              "Expression cannot be empty",
              SeverityLevel.Error
            )
        case _ =>
          diagnostics += Diagnostic(
            "properties.renameMethod",
            "Invalid method to rename columns",
            SeverityLevel.Error
          )
      }
    } else {
      diagnostics += Diagnostic(
            "properties.renameMethod",
            "Please select a method to rename columns.",
            SeverityLevel.Error
          )
    }
    

    diagnostics.toList
  }
  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newSchema = newState.ports.inputs.head.schema
    val newProperties = newState.properties.copy(schema = newSchema)
    newState.copy(properties = newProperties)
  }

  def deserializeProperty(props: String): RenameColumnsProperties = Json.parse(props).as[RenameColumnsProperties]

  def serializeProperty(props: RenameColumnsProperties): String = Json.toJson(props).toString()

  class RenameColumnsCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
    //  import org.apache.spark.sql.ProphecyDataFrame

    //  val expressionForRename = props.renameMethod match {
    //    case "Add Prefix" => "concat('" + props.prefix.get + "',column_name)"
    //    case "Add Suffix" => "concat(column_name,'" + props.suffix.get + "')"
    //    case _            => props.customExpression.get
    //  }
    //  ProphecyDataFrame.extendedDataFrame(in).evaluate_expression(expressionForRename, props.columnNames.getOrElse(Nil), spark)
    in
    }
  }
}
