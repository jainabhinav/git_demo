package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}
class MultiFieldExpression extends ComponentSpec {

  val name: String = "MultiFieldExpression"
  val category: String = "Transform"
  type PropertiesType = MultiFieldExpressionProperties
  override def optimizeCode: Boolean = true
  case class MultiFieldExpressionProperties(
    @Property("columnNames", "Selected Columns")
    columnNames: Option[List[String]] = None,
    @Property("schema")
    schema: Option[StructType] = None,
    @Property("Data Type")
    dataType: String = "",
    @Property("Add prefix / suffix")
    prefixSuffixOption: String = "Prefix / Suffix to be added",
    @Property("Actual value of prefix / suffix")
    prefixSuffixToBeAdded: String = "",
    @Property("Cast Output")
    castOutputTypeName: String = "Select output type",
    @Property("changeOutputFieldName", "Copy Output fields and Add")
    changeOutputFieldName: Boolean = false,
    @Property("changeOutputFieldType", "Change Output Type to")
    changeOutputFieldType: Boolean = false,
    @Property("expressionToBeApplied", "Expression")
    expressionToBeApplied: String = "",
    @Property("isPrefix", "prefix boolean")
    isPrefix: Boolean = false
  ) extends ComponentProperties

  implicit val MultiFieldExpressionPropertiesFormat: Format[MultiFieldExpressionProperties] = Json.format

  def dialog: Dialog = {

    val typeNameToType = List(
      "StringType",
      "BinaryType",
      "BooleanType",
      "ByteType",
      "ShortType",
      "IntegerType",
      "LongType",
      "FloatType",
      "DoubleType",
      "DateType",
      "TimestampType"
    )

    val dataTypeSelectBox = SelectBox("Data Type of the columns to do operations on")
      .addOption("String Type", "String")
      .addOption("Numeric Type", "Numeric")
      .addOption("Date/Timestamp Type", "Date")
      .bindProperty("dataType")

    val prefixSuffixDropDown = SelectBox("Add prefix / Suffix")
      .addOption("Prefix", "Prefix")
      .addOption("Suffix", "Suffix")
      .bindProperty("prefixSuffixOption")

    val sparkDataTypeList =
      typeNameToType
        .foldLeft(SelectBox("Cast output column as"))((acc, cc) => acc.addOption(cc, cc))
        .bindProperty("castOutputTypeName")

    Dialog("MultiFieldExpression").addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(Ports(), "content")
        .addColumn(
          StackLayout(height = Some("100%"))
            .addElement(dataTypeSelectBox)
            .addElement(
              SchemaColumnsDropdown("Select Columns")
                .withMultipleSelection()
                .bindSchema("schema")
                .bindProperty("columnNames")
            )
            .addElement(
              Checkbox("Change output column name")
                .bindProperty("changeOutputFieldName")
            )
            .addElement(
              Condition()
                .ifEqual(PropExpr("component.properties.changeOutputFieldName"), BooleanExpr(true))
                .then(
                  ColumnsLayout(gap = Some("1rem"))
                    .addColumn(prefixSuffixDropDown)
                    .addColumn(
                      TextBox("Value")
                        .bindPlaceholder("""Example: new_""")
                        .bindProperty("prefixSuffixToBeAdded")
                    )
                )
            )
            .addElement(
              Checkbox("Change output column type")
                .bindProperty("changeOutputFieldType")
            )
            .addElement(
              Condition()
                .ifEqual(PropExpr("component.properties.changeOutputFieldType"), BooleanExpr(true))
                .then(
                  ColumnsLayout()
                    .addColumn(sparkDataTypeList)
                )
            )
            .addElement(
              ExpressionBox("Output Expression")
                .bindProperty("expressionToBeApplied")
                .bindPlaceholder(
                  """Write spark sql expression considering `column_value` as column name and `column_name` as column name string literal. Example:
                    |For column value: column_value * 100
                    |For column name: upper(column_name)""".stripMargin)
                .bindLanguage("plaintext")
            )
        )
    )
  }

  def validate(component: Component)(implicit
    context: WorkflowContext
  ): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val outputTypePlaceholderValue = "Select output type"
    val prefixSuffixPlaceholderValue = "Prefix / Suffix to be added"
    val dataTypeOfColumnsPlaceholderValue = ""
    val diagnostics = ListBuffer[Diagnostic]()
    if (component.properties.dataType.equals(dataTypeOfColumnsPlaceholderValue)) {
      diagnostics += Diagnostic(
        "properties.dataType",
        "Please choose a valid input data type",
        SeverityLevel.Error
      )
    } else {
      if ((!component.properties.columnNames.isDefined) || (component.properties.columnNames.get.size == 0)) {
        diagnostics += Diagnostic(
          "properties.columnNames",
          "At least one column needs to be selected",
          SeverityLevel.Error
        )
      }

      if (component.properties.expressionToBeApplied.isEmpty) {
          diagnostics += Diagnostic(
            "properties.expressionToBeApplied",
            "Expression to be applied cannot be empty",
            SeverityLevel.Error
          )
        }

        if (component.properties.changeOutputFieldName && component.properties.prefixSuffixToBeAdded.isEmpty) {
          diagnostics += Diagnostic(
            "properties.prefixSuffixToBeAdded",
            "Prefix / Suffix value cannot be empty",
            SeverityLevel.Error
          )
        }

        if (
          component.properties.changeOutputFieldName && component.properties.prefixSuffixOption.equals(
            prefixSuffixPlaceholderValue
          )
        ) {
          diagnostics += Diagnostic(
            "properties.prefixSuffixOption",
            "Please choose a valid option for prefix / suffix",
            SeverityLevel.Error
          )
        }

        if (
          component.properties.changeOutputFieldType && component.properties.castOutputTypeName.equals(
            outputTypePlaceholderValue
          )
        ) {
          diagnostics += Diagnostic(
            "properties.castOutputTypeName",
            "Please choose a valid output spark type",
            SeverityLevel.Error
          )
        }
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit
    context: WorkflowContext
  ): Component = {
    val dataTypeMapping: Map[String, Set[String]] = Map(
      "String" -> Set("StringType"),
      "Numeric" -> Set("ByteType", "ShortType", "IntegerType", "LongType", "FloatType", "DoubleType"),
      "Date" -> Set("DateType", "TimestampType")
    )
    val selectedDataTypes = dataTypeMapping.getOrElse(newState.properties.dataType, Set())
    println(s"selected ${selectedDataTypes}")
    val newSchema = newState.ports.inputs.head.schema.map(value =>
      StructType(value.fields.filter(x => selectedDataTypes.contains(x.dataType.toString)))
    )
    val prefix = newState.properties.prefixSuffixOption.equals("Prefix")
    val newProperties = newState.properties.copy(schema = newSchema, isPrefix = prefix)
    newState.copy(properties = newProperties)
  }

  def deserializeProperty(props: String): MultiFieldExpressionProperties =
    Json.parse(props).as[MultiFieldExpressionProperties]

  def serializeProperty(props: MultiFieldExpressionProperties): String =
    Json.toJson(props).toString()

  class MultiFieldExpressionCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {

//      val selectExpressions = props.columnNames.getOrElse(Nil).map { columnName =>
//        val alias =
//          if (props.changeOutputFieldName && props.isPrefix)
//            props.prefixSuffixToBeAdded + columnName
//          else if (props.changeOutputFieldName && !props.isPrefix)
//            columnName + props.prefixSuffixToBeAdded
//          else columnName
//
//        if (props.changeOutputFieldType) {
//
//          val castType: DataType = props.castOutputTypeName match {
//            case "StringType" ⇒ StringType
//            case "BinaryType" ⇒ BinaryType
//            case "BooleanType" ⇒ BooleanType
//            case "ByteType" ⇒ ByteType
//            case "ShortType" ⇒ ShortType
//            case "IntegerType" ⇒ IntegerType
//            case "LongType" ⇒ LongType
//            case "FloatType" ⇒ FloatType
//            case "DoubleType" ⇒ DoubleType
//            case "DateType" ⇒ DateType
//            case "TimestampType" ⇒ TimestampType
//            case _ ⇒ StringType
//          }
//          expr(
//            props.expressionToBeApplied
//              .replace("column_value", columnName)
//              .replace("column_name", "'" + columnName + "'")
//          ).as(alias).cast(castType)
//        } else {
//          expr(
//            props.expressionToBeApplied
//              .replace("column_value", columnName)
//              .replace("column_name", "'" + columnName + "'")
//          ).as(alias)
//        }
//
//      }
//      val inColumns = in.columns.toSet
//      val remainingColumns = inColumns -- props.columnNames.getOrElse(Nil).toSet
//      val remainingExpressions = remainingColumns.map(col)
//      val allExpressions = selectExpressions ++ remainingExpressions
//      in.select(allExpressions: _*)
      in
    }
  }
}
