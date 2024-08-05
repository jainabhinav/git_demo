package io.prophecy.core.instructions.all

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._
import sdl.rest.CodeLanguage

import scala.collection.immutable._
class TestAnandDQV extends ComponentSpec {
  val name: String = "TestAnandDQV"
  val category: String = "Custom"
  override def optimizeCode: Boolean = true
  type PropertiesType = TestAnandDQVProperties

  

  implicit val formatTestAnandDQVProperties: Format[TestAnandDQVProperties] =
    Jsonx.formatCaseClass[TestAnandDQVProperties]
  implicit val formatCompletenessRules: Format[CompletenessRules] = Jsonx.formatCaseClass[CompletenessRules]
  implicit val formatUniquenessRules: Format[UniquenessRules] = Jsonx.formatCaseClass[UniquenessRules]
  implicit val formatDistinctCountRules: Format[DistinctCountRules] = Jsonx.formatCaseClass[DistinctCountRules]
  implicit val formatComplianceRules: Format[ComplianceRules] = Jsonx.formatCaseClass[ComplianceRules]
  implicit val formatDataTypeRules: Format[DataTypeRules] = Jsonx.formatCaseClass[DataTypeRules]
  implicit val formatAnomalyRules: Format[AnomalyRules] = Jsonx.formatCaseClass[AnomalyRules]
  implicit val formatAllowedListOfValuesRules: Format[AllowedListOfValuesRules] =
    Jsonx.formatCaseClass[AllowedListOfValuesRules]
  implicit val formatRangeRules: Format[RangeRules] = Jsonx.formatCaseClass[RangeRules]
  implicit val formatWindowAnomalyRules: Format[WindowAnomalyRules] = Jsonx.formatCaseClass[WindowAnomalyRules]
  override def deserializeProperty(props: String): TestAnandDQVProperties =
    Json.parse(props).as[TestAnandDQVProperties]

  override def serializeProperty(props: TestAnandDQVProperties): String =
    Json.stringify(Json.toJson(props))
  case class TestAnandDQVProperties(
    @Property("Last open tab")
    activeTab: String = "completenessTab",
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Rule setup options")
    ruleSetupSelection: String = "table",
    @Property("Completeness rules")
    completenessRules: List[CompletenessRules] = Nil,
    @Property("Uniqueness rules")
    uniquenessRules: List[UniquenessRules] = Nil,
    @Property("DistinctCount rules")
    distinctCountRules: List[DistinctCountRules] = Nil,
    @Property("Compliance rules")
    complianceRules: List[ComplianceRules] = Nil,
    @Property("DataType rules")
    dataTypeRules: List[DataTypeRules] = Nil,
    @Property("Anomaly rules")
    anomalyRules: List[AnomalyRules] = Nil,
    @Property("Allowed list of values rules")
    allowedListOfValuesRules: List[AllowedListOfValuesRules] = Nil,
    @Property("Range rules")
    rangeRules: List[RangeRules] = Nil,
    @Property("Window Anomaly rules")
    windowAnomalyRules: List[WindowAnomalyRules] = Nil,
    @Property("Rules Json")
    rulesJson: String = "{}",
    @Property("Entity Name")
    entityName: String = "",
    @Property("Generate Json From Table")
    generateFromTable: Boolean = true
  ) extends ComponentProperties


  // @Property("Completeness Table")
  // case class SRawInt(
  //   @Property("ColumnName") columnName: String
  // )

  @Property("Completeness Table")
  case class CompletenessRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("Threshold") threshold: SDouble = SDouble("1.0")
  )

  @Property("Uniqueness Table")
  case class UniquenessRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true
  )

  @Property("DistinctCount Table")
  case class DistinctCountRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("DistinctValuesCount") distinctValues: SInt
  )

  // TODO: fix datatype for priority
  @Property("Allowed List of values Table")
  case class AllowedListOfValuesRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("Allowed List of Values") enumValues: String = ""
  )

  @Property("Range Table")
  case class RangeRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("LowerLimit") lowerLimit: Option[SDouble] = None,
    @Property("UpperLimit") upperLimit: Option[SDouble] = None
  )

  @Property("DataType Table")
  case class DataTypeRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("DataType") dataType: String
  )

  @Property("Compliance Table")
  case class ComplianceRules(
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("Predicate") predicate: Option[String] = None,
    @Property("Threshold") threshold: SDouble = SDouble("1.0")
  )

  @Property("Anomaly Table")
  case class AnomalyRules(
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("Predicate") predicate: Option[String] = None,
    @Property("Strategy") strategy: String = "Relative",
    @Property("LowerLimit") lowerLimit: SDouble = SDouble("0"),
    @Property("UpperLimit") upperLimit: SDouble = SDouble("0")
  )

  @Property("Window Anomaly Table")
  case class WindowAnomalyRules(
    @Property("ColumnName") columnName: String,
    @Property("RuleName") ruleName: String = "",
    @Property("Severity") checkLevel: String = "Error",
    @Property("Priority") priority: SInt = SInt("0"),
    @Property("Enabled") isEnabled: Boolean = true,
    @Property("Predicate") predicate: String,
    @Property("WindowDays") windowDays: String,
    @Property("LowerLimit") lowerLimit: SDouble = SDouble("0"),
    @Property("UpperLimit") upperLimit: SDouble = SDouble("0"),
    @Property("GroupByColumn") groupByColumn: String,
    @Property("Analyzer") analyzer: String = "Size"
  )

  def dialog: Dialog = {

    val configurationMethodsSelectBox = SelectBox("How would you like to configure the data quality checks")
      .addOption("Inline table", "table")
      .addOption("Rules Json", "json")

    val strategySelectBox = SelectBox("")
      .addOption("Relative", "RelativeChangeStrategy")
      .addOption("Absolute", "AbsoluteChangeStrategy")

    val severitySelectBox = SelectBox("")
      .addOption("Error", "Error")
      .addOption("Warning", "Warning")

    val prioritySelectBox = SelectBox("")
      .addOption("0", "0")
      .addOption("1", "1")
      .addOption("2", "2")
      .addOption("3", "3")

    val dataTypeSelectBox = SelectBox("")
      .addOption("Integer", "integer")
      .addOption("Fraction", "fraction")
      .addOption("Numeric", "numeric")
      .addOption("Boolean", "boolean")
      .addOption("String", "string")

    val analyzerSelectBox = SelectBox("")
      .addOption("Size", "Size")
      .addOption("Sum", "Sum")
      .addOption("CountDistinct", "CountDistinct")

    /* Common table column definitions */
    val columnNameColumn = Column("Column", "columnName", Some(TextBox("").bindPlaceholder("Column")), width = "30%")

    val ruleNamePlaceholderText = "Describes the data quality check"
    val ruleNameControlBox = Some(
      ExpressionBox(ignoreTitle = true).bindPlaceholder(ruleNamePlaceholderText).bindLanguage("text")
    )
    val ruleNameColumn = Column("Rule Name", "ruleName", ruleNameControlBox, width = "50%")
    //    val predicateControl = Some(ExpressionBox(ignoreTitle = true)
    //      .bindLanguage("sql")
    //      .withSchemaSuggestions()
    //      .bindPlaceholders(
    //        mutable
    //          .Map(
    //            "scala" → """""",
    //            "python" → """""",
    //            "sql" → """col1 > 4"""
    //          )
    //      ))

    val severityCheckColumn = Column("Severity", "checkLevel", Some(severitySelectBox), width = "15%")

    val priorityColumn = Column("Priority", "priority", Some(prioritySelectBox), width = "15%")

    val enabledColumn =
      Column("Enabled", "isEnabled", Some(Checkbox("").bindProperty("record.isEnabled")), width = "15%")

    val dataTypeColumn = Column("DataType", "dataType", Some(dataTypeSelectBox), width = "15%")

    val analyzerColumn = Column("Analayzer", "analyzer", Some(analyzerSelectBox), width = "20%")

    /* Table definitions */
    val completenessRulesTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        Column("Threshold", "threshold", Some(TextBox("").bindPlaceholder("1.0")), width = "15%"),
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val uniquenessRulesTable = BasicTable(
      "",
      columns = List(columnNameColumn, ruleNameColumn, severityCheckColumn, priorityColumn, enabledColumn)
    )

    val distinctCountRulesTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        Column(
          "Distinct Values Count",
          "distinctValues",
          Some(TextBox("").bindPlaceholder("Count of distinct values")),
          width = "15%"
        ),
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val rangeRulesTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        Column("Lower Limit", "lowerLimit", Some(TextBox("").bindPlaceholder("lower limit")), width = "12.5%"),
        Column("Upper Limit", "upperLimit", Some(TextBox("").bindPlaceholder("upper limit")), width = "12.5%"),
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val allowedListOfValuesColumnPlaceholder = "Enter the allowed list of values with each value on a separate line"
    val allowedListOfValuesColumn = Column(
      "Enums",
      "enumValues",
      Some(TextArea("", 2, placeholder = allowedListOfValuesColumnPlaceholder)),
      width = "35%"
    )
    val allowedListOfValuesRuleTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        allowedListOfValuesColumn,
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val dataTypeRulesTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        dataTypeColumn,
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val complianceRuleTable = BasicTable(
      "",
      columns = List(
        ruleNameColumn,
        Column("Predicate", "predicate", Some(TextBox("").bindPlaceholder("col1 > 4")), width = "30%"),
        Column("Threshold", "threshold", Some(TextBox("").bindPlaceholder("1.0")), width = "15%"),
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val anomalyRuleTable = BasicTable(
      "",
      columns = List(
        ruleNameColumn,
        Column("Strategy", "strategy", Some(strategySelectBox), width = "15%"),
        Column("Lower Limit", "lowerLimit", Some(TextBox("").bindPlaceholder("lower")), width = "17.5%"),
        Column("Upper Limit", "upperLimit", Some(TextBox("").bindPlaceholder("upper")), width = "17.5%"),
        Column("Predicate", "predicate", Some(TextBox("").bindPlaceholder("col1 > 4")), width = "30%"),
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val windowAnomalyRuleTable = BasicTable(
      "",
      columns = List(
        columnNameColumn,
        ruleNameColumn,
        Column("Lower Threshold", "lowerLimit", Some(TextBox("")), width = "17.5%"),
        Column("Upper Threshold", "upperLimit", Some(TextBox("")), width = "17.5%"),
        Column("Predicate", "predicate", Some(TextBox("").bindPlaceholder("col1 > 4")), width = "30%"),
        Column("Window Days", "windowDays", Some(TextBox("")), width = "15%"),
        Column("Group By Column", "groupByColumnName", Some(TextBox("")), width = "25%"),
        analyzerColumn,
        severityCheckColumn,
        priorityColumn,
        enabledColumn
      )
    )

    val windowTabs = Tabs()
      .bindProperty("activeTab")
      .addTabPane(
        TabPane("Completeness", "completenessTab").addElement(completenessRulesTable.bindProperty("completenessRules"))
      )
      .addTabPane(
        TabPane("Uniqueness", "uniquenessTab").addElement(uniquenessRulesTable.bindProperty("uniquenessRules"))
      )
      .addTabPane(
        TabPane("Distinct Count", "distinctCountTab").addElement(
          distinctCountRulesTable.bindProperty("distinctCountRules")
        )
      )
      .addTabPane(TabPane("Range", "rangeTab").addElement(rangeRulesTable.bindProperty("rangeRules")))
      .addTabPane(
        TabPane("Enums", "allowedListOfValuesTab").addElement(
          allowedListOfValuesRuleTable.bindProperty("allowedListOfValuesRules")
        )
      )
      .addTabPane(TabPane("Data Type", "dataTypeTab").addElement(dataTypeRulesTable.bindProperty("dataTypeRules")))
      .addTabPane(
        TabPane("Compliance", "complianceTab").addElement(complianceRuleTable.bindProperty("complianceRules"))
      )
      .addTabPane(TabPane("Anomaly", "anomalyTab").addElement(anomalyRuleTable.bindProperty("anomalyRules")))
    // .addTabPane(TabPane("WindowAnomaly", "windowAnomalyTab").addElement(windowAnomalyRuleTable.bindProperty("windowAnomalyRules")))

    Dialog("DeequDataQuality")
      .addElement(
        ColumnsLayout(height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              allowInportRename = true,
              allowOutportRename = true,
              allowOutportAddDelete = true,
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                var newCompletenessRules: List[CompletenessRules] = Nil
                var newUniquenessRules: List[UniquenessRules] = Nil
                var newDistinctCountRules: List[DistinctCountRules] = Nil
                var newAllowedListOfValuesRules: List[AllowedListOfValuesRules] = Nil
                var newComplianceRules: List[ComplianceRules] = Nil
                var newDataTypeRules: List[DataTypeRules] = Nil
                var newRangeRules: List[RangeRules] = Nil
                var newAnomalyRules: List[AnomalyRules] = Nil
                var newWindowAnomalyRules: List[WindowAnomalyRules] = Nil

                st.properties.activeTab match {
                  case "completenessTab" ⇒
                    newCompletenessRules = List(
                      CompletenessRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"Completeness Check for ${sanitizedColumn(column)}"
                      )
                    )
                  case "uniquenessTab" ⇒
                    newUniquenessRules = List(
                      UniquenessRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"Uniqueness Check for ${sanitizedColumn(column)}"
                      )
                    )
                  case "distinctCountTab" ⇒
                    newDistinctCountRules = List(
                      DistinctCountRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"DistinctCount Check for ${sanitizedColumn(column)}",
                        distinctValues = SInt("")
                      )
                    )
                  case "allowedListOfValuesTab" ⇒
                    newAllowedListOfValuesRules = List(
                      AllowedListOfValuesRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"${sanitizedColumn(column)} can only have one of the specified",
                        enumValues = ""
                      )
                    )
                  case "rangeTab" ⇒
                    newRangeRules = List(
                      RangeRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"${sanitizedColumn(column)} value should lie between the specified range"
                      )
                    )
                  case "dataTypeTab" ⇒
                    newDataTypeRules = List(
                      DataTypeRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = s"Datatype Check for ${sanitizedColumn(column)}",
                        dataType = ""
                      )
                    )
                  case "windowAnomalyTab" ⇒
                    newWindowAnomalyRules = List(
                      WindowAnomalyRules(
                        columnName = s"${sanitizedColumn(column)}",
                        ruleName = "",
                        predicate = "",
                        windowDays = "",
                        lowerLimit = SDouble(""),
                        upperLimit = SDouble(""),
                        groupByColumn = ""
                      )
                    )

                  case "complianceTab" | "anomalyTab" ⇒

                }

                def distinctBy[T, K](list: List[T])(f: T => K): List[T] = {
                  list
                    .foldLeft((List.empty[T], Set.empty[K])) {
                      case ((acc, set), elem) =>
                        val key = f(elem)
                        if (set.contains(key)) (acc, set)
                        else (elem :: acc, set + key)
                    }
                    ._1
                    .reverse
                }

                val newProperties = st.properties.copy(
                  columnsSelector = (st.properties.columnsSelector ::: List(s"$portId##$column")).distinct,
                  uniquenessRules = distinctBy(st.properties.uniquenessRules ::: newUniquenessRules)(_.columnName),
                  completenessRules =
                    distinctBy(st.properties.completenessRules ::: newCompletenessRules)(_.columnName),
                  distinctCountRules =
                    distinctBy(st.properties.distinctCountRules ::: newDistinctCountRules)(_.columnName),
                  allowedListOfValuesRules =
                    distinctBy(st.properties.allowedListOfValuesRules ::: newAllowedListOfValuesRules)(_.columnName),
                  rangeRules = distinctBy(st.properties.rangeRules ::: newRangeRules)(_.columnName),
                  dataTypeRules = distinctBy(st.properties.dataTypeRules ::: newDataTypeRules)(_.columnName),
                  windowAnomalyRules =
                    distinctBy(st.properties.windowAnomalyRules ::: newWindowAnomalyRules)(_.columnName)
                )

                st.copy(properties = newProperties)
              }
            ).importSchema(),
            "2fr"
          )
          .addColumn(
            StackLayout(height = Some("90%"))
              .addElement(configurationMethodsSelectBox.bindProperty("ruleSetupSelection"))
              .addElement(
                Condition()
                  .ifNotEqual((PropExpr("component.properties.ruleSetupSelection")), (StringExpr("table")))
                  .then(Checkbox("Autogenerate from Inline Table").bindProperty("generateFromTable"))
              )
              .addElement(
                Condition()
                  .ifEqual((PropExpr("component.properties.ruleSetupSelection")), (StringExpr("table")))
                  .then(windowTabs)
                  .otherwise(Editor(height = Some("800px")).bindLanguage("json").bindProperty("rulesJson"))
              ),
            "5fr"
          )
      )
  }

  // TODO: Add validations wherever datatype validations are not enough
  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    for (rule <- component.properties.distinctCountRules) {
      val (diag, limit) = (rule.distinctValues.diagnostics, rule.distinctValues.value)
      diagnostics ++= diag
    }

    for (rule <- component.properties.rangeRules) {

      if (rule.lowerLimit.isEmpty && rule.upperLimit.isEmpty) {
        diagnostics += Diagnostic(
          "rule.lowerLimit",
          "Either lower Limit or upper Limit must be defined",
          SeverityLevel.Error
        )
      }

      if (rule.lowerLimit.isDefined) {
        val (diag, limit) = (rule.lowerLimit.get.diagnostics, rule.lowerLimit.get.value)
        diagnostics ++= diag
      }
      if (rule.upperLimit.isDefined) {
        val (diag2, limit2) = (rule.upperLimit.get.diagnostics, rule.upperLimit.get.value)
        diagnostics ++= diag2
      }

      if (
        rule.lowerLimit.isDefined && rule.upperLimit.isDefined
        && rule.lowerLimit.get.rawValue > rule.upperLimit.get.rawValue
      ) {
        diagnostics += Diagnostic(
          "rule.lowerLimit",
          "Lower limit cannot be greater than upper limit",
          SeverityLevel.Error
        )
      }

    }

    for ((rule, idx) <- component.properties.complianceRules.zipWithIndex) {
      //   if(rule.predicate.isDefined){
      val diag = validateSColumn(
        SColumn(CodeLanguage.sql, rule.predicate.get),
        s"properties.complianceRules[$idx].predicate",
        component,
        Some(ColumnsUsage.WithoutInputAlias)
      ).map(_.copy(path = s"properties.complianceRules[$idx].predicate"))
      diagnostics ++= diag
      //   }

    }

    for ((rule, idx) <- component.properties.anomalyRules.zipWithIndex) {

      if (rule.predicate.isDefined && !rule.predicate.get.isEmpty) {
        val diag = validateSColumn(
          SColumn(CodeLanguage.sql, rule.predicate.get),
          s"properties.anomalyRules[$idx].predicate",
          component,
          Some(ColumnsUsage.WithoutInputAlias)
        ).map(_.copy(path = s"properties.anomalyRules[$idx].predicate"))
        diagnostics ++= diag
      }
    }
    diagnostics.toList

  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    import play.api.libs.json._

    import scala.collection.mutable.ListBuffer
    import scala.util.matching.Regex
    import scala.util.Try

    implicit val sdoubleWrites: Writes[SDouble] = new Writes[SDouble] {
      override def writes(sdouble: SDouble): JsValue = JsNumber(BigDecimal(sdouble.rawValue))
    }

    implicit val sintWrites: Writes[SInt] = new Writes[SInt] {
      override def writes(sint: SInt): JsValue = JsNumber(BigDecimal(sint.rawValue))
    }

    implicit val scolumnWrites: Writes[SColumn] = new Writes[SColumn] {
      override def writes(scolumn: SColumn): JsValue = JsString(scolumn.expression)
    }

    
    implicit val completenessWrites: Writes[CompletenessRules] = Json.writes[CompletenessRules]
    implicit val listcompletenessWrites: Writes[List[CompletenessRules]] = Writes.list(completenessWrites)


    implicit val uniquenessWrites: Writes[UniquenessRules] = Json.writes[UniquenessRules]
    implicit val listuniquenessWrites: Writes[List[UniquenessRules]] = Writes.list(uniquenessWrites)


    implicit val distinctCountWrites: Writes[DistinctCountRules] = Json.writes[DistinctCountRules]
    implicit val listdistinctCountWrites: Writes[List[DistinctCountRules]] = Writes.list(distinctCountWrites)

    implicit val enumWrites: Writes[AllowedListOfValuesRules] = new Writes[AllowedListOfValuesRules] {
      override def writes(enumRules: AllowedListOfValuesRules): JsValue = Json.obj(
        "columnName" -> enumRules.columnName,
        "ruleName" -> enumRules.ruleName,
        "checkLevel" -> enumRules.checkLevel,
        "isEnabled" -> enumRules.isEnabled,
        "priority" -> enumRules.priority,
        "enumValues" -> enumRules.enumValues.split("[,\n]").toList
      )
    }

    implicit val listenumWrites: Writes[List[AllowedListOfValuesRules]] = Writes.list(enumWrites)

    
    implicit val complianceWrites: Writes[ComplianceRules] = Json.writes[ComplianceRules]
    implicit val listcomplianceWrites: Writes[List[ComplianceRules]] = Writes.list(complianceWrites)


    implicit val dataTypeWrites: Writes[DataTypeRules] = Json.writes[DataTypeRules]
    implicit val listdataTypeWrites: Writes[List[DataTypeRules]] = Writes.list(dataTypeWrites)


    implicit val rangeWrites: Writes[RangeRules] = Json.writes[RangeRules]
    implicit val listrangeWrites: Writes[List[RangeRules]] = Writes.list(rangeWrites)


    implicit val anomalyWrites: Writes[AnomalyRules] = Json.writes[AnomalyRules]
    implicit val listanomalyWrites: Writes[List[AnomalyRules]] = Writes.list(anomalyWrites)


    implicit val windowAnomalyWrites: Writes[WindowAnomalyRules] = Json.writes[WindowAnomalyRules]
    implicit val listwindowAnomalyWrites: Writes[List[WindowAnomalyRules]] = Writes.list(windowAnomalyWrites)

    val newProps = newState.properties
    var usedColExps = newState.properties.columnsSelector
    var anomalyRules = ListBuffer[AnomalyRules]() ++= newState.properties.anomalyRules
    var rulesJson: String = newState.properties.rulesJson
    val previousAutoGenerateFlag: Boolean = oldState.properties.generateFromTable
    val previousTab = oldState.properties.ruleSetupSelection
    // TODO: Autogenerate rule names based on regex and rule properties
    val entityName: String = newState.ports.inputs(0).slug.toString

    newState.properties.ruleSetupSelection match {
      case "table" =>
        newState.properties.activeTab match {
          case "completenessTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.completenessRules.map(_.columnName), newState)
          case "uniquenessTab" =>
            usedColExps = getColumnsToHighlight(newProps.uniquenessRules.map(_.columnName), newState)
          case "distinctCountTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.distinctCountRules.map(_.columnName), newState)
          case "allowedListOfValuesTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.allowedListOfValuesRules.map(_.columnName), newState)
          case "rangeTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.rangeRules.map(_.columnName), newState)
          case "dataTypeTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.dataTypeRules.map(_.columnName), newState)
          case "windowAnomalyTab" ⇒
            usedColExps = getColumnsToHighlight(newProps.windowAnomalyRules.map(_.columnName), newState)
          case "anomalyTab" =>
            usedColExps = getColumnsToHighlight(newProps.anomalyRules.map(_.predicate.get), newState)

            val anomalyRuleNamePattern: Regex =
              """(\s+)|(^(The).*(rate of change for the expression - ').*(' should be within the range).*$)""".r

            anomalyRules.clear()
            for (rule ← newState.properties.anomalyRules) {
              rule.ruleName match {
                case anomalyRuleNamePattern(_*) =>
                  anomalyRules += rule.copy(
                    ruleName = s"The ${rule.strategy} rate of change for the expression - '${rule.predicate}' should be within the range (${rule.lowerLimit.rawValue}, ${rule.upperLimit.rawValue})"
                  )
                case _ =>
                  anomalyRules += rule
              }
            }
          case "complianceTab" =>
            usedColExps = getColumnsToHighlight(newProps.complianceRules.map(_.predicate.get), newState)
          case _ =>

        }

        if (newState.properties.generateFromTable) {
          rulesJson = Try {
            Json
              .prettyPrint(
                Json.obj(
                  "entityName" -> entityName,
                  "version" -> 2.0,
                  "validationRules" -> Json.obj(
                    "completeness" -> Json.toJson(newState.properties.completenessRules),
                    "uniqueness" -> Json.toJson(newState.properties.uniquenessRules),
                    "distinctCount" -> Json.toJson(newState.properties.distinctCountRules),
                    "compliance" -> Json.toJson(newState.properties.complianceRules),
                    "dataType" -> Json.toJson(newState.properties.dataTypeRules),
                    "anomaly" -> Json.toJson(newState.properties.anomalyRules),
                    "enum" -> Json.toJson(newState.properties.allowedListOfValuesRules),
                    "range" -> Json.toJson(newState.properties.rangeRules)
                    // "windowsAnomaly" -> Json.toJson(newState.properties.windowAnomalyRules)
                  )
                )
              )
              .toString
          }.getOrElse(rulesJson)
        }

      case "json" =>
        if (!previousAutoGenerateFlag && newState.properties.generateFromTable) {
          rulesJson = Try {
            Json
              .prettyPrint(
                Json.obj(
                  "entityName" -> entityName,
                  "version" -> 2.0,
                  "validationRules" -> Json.obj(
                    "completeness" -> Json.toJson(newState.properties.completenessRules),
                    "uniqueness" -> Json.toJson(newState.properties.uniquenessRules),
                    "distinctCount" -> Json.toJson(newState.properties.distinctCountRules),
                    "compliance" -> Json.toJson(newState.properties.complianceRules),
                    "dataType" -> Json.toJson(newState.properties.dataTypeRules),
                    "anomaly" -> Json.toJson(newState.properties.anomalyRules),
                    "enum" -> Json.toJson(newState.properties.allowedListOfValuesRules),
                    "range" -> Json.toJson(newState.properties.rangeRules)
                    // "windowsAnomaly" -> Json.toJson(newState.properties.windowAnomalyRules)
                  )
                )
              )
              .toString
          }.getOrElse(rulesJson)
        }

    }
    val nodePorts =
      newState.ports.copy(inputs = newState.ports.inputs, outputs = newState.ports.inputs, isCustomOutputSchema = true)

    newState.copy(
      properties = newProps.copy(
        columnsSelector = usedColExps,
        anomalyRules = anomalyRules.toList,
        rulesJson = rulesJson,
        entityName = entityName
      ),
      ports = nodePorts
    )
  }

  override def userReadme: String =
    """
      |
      |## Supported Data Quality Checks  
      |  
      |The supported DQ checks along with their respective input parameters are listed below:  
      |  
      |- **Completeness** - Checks for the fraction of non-null values in a column and compares it with the specified threshold.  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | --------- | --------------------------- | --------------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      || threshold | Double | No (Default Value: 1.0) | Minimum fraction of rows for which check should hold true |  
      |  
      |</br>  
      |</br>  
      |  
      |- **Uniqueness** - Checks if all the values for the specified column are unique or not. Unique values exists only once.  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | --------- | --------------------------- | ------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      |  
      |</br>  
      |</br>  
      |  
      |- **DistinctCount** - Verifies whether the column contains the specified count of distinct values, as indicated in the "distinctValues" field. Distinct values occur at least once.  
      |  
      || Parameter | Data Type | Required? | Description |  
      || -------------- | --------- | ----------------------------- | --------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      || distinctValues | Integer | Yes | Number of exact distinct values |  
      |  
      |</br>  
      |</br>  
      |  
      |- **Range** - For a given column, this check verifies if the range of values for that column is within the specified threshold (lowerLimit/upperLimit).  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | --------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      || lowerLimit | Double | Yes | Specify the acceptable lower and upper bound values. These params are optional but at least one of them MUST be present |  
      || upperLimit | Double | Yes | Specify the acceptable lower and upper bound values. These params are optional but at least one of them MUST be present |  
      |  
      |</br>  
      |</br>  
      |  
      |- **Enum** - The set of values that are acceptable for the column.  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | ------------- | --------------------------- | ---------------------------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      || enumValues | List (String) | Yes | Specify the set of values that are acceptable for the enum type column |  
      |  
      |</br>  
      |</br>  
      |  
      |- **DataType** - The data type of the values that are acceptable for the column.  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | --------- | --------------------------- | ------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || columnName | String | Yes | Specify on which column this check should run |  
      || dataType | String | Yes | Specify the acceptable datatype |  
      |  
      |</br>  
      |</br>  
      |  
      |- **Anomaly** - Comprises all checks that compare the current result with the previous run. This check can be customized by inserting any SQL query in the "predicate" field, and the checker will check whether the number of records satisfying the given predicate changes over the previous run changes outside the specified lower/upper bounds. If threshold(strategy) is relative, lower/upper should be a ratio (e.g., 0.9/1.1). If threshold(strategy) is absolute, lower/upper should be the absolute numbers (e.g.,-500/500).  
      |  
      || Parameter | Data Type | Required? | Description |  
      || ---------- | --------- | --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |  
      || ruleName | String | Yes | Human-readable name of check |  
      || checkLevel | String | No (Default Value: "Error") | Severity level for the check |  
      || isEnabled | Boolean | No (Default Value: true) | Specify Boolean value to enable/disable the check |  
      || priority | Integer | No (Default Value: 0) | Specify the priority of the check |  
      || lowerLimit | Double | Yes | Specify the acceptable lower and upper bound values. These params are optional but at least one of them MUST be present |  
      || upperLimit | Double | Yes | Specify the acceptable lower and upper bound values. These params are optional but at least one of them MUST be present |  
      || predicate | String | No | Any grammatically correct SQL string that can run without error on specified dataframe, will throw error if SQL has syntax error or refers to columns not existing in the referred dataframe |  
      |  
      |</br>  
      |</br>"""

  class DeequDataQualityCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      // import com.amazon.deequ.repository.ResultKey
      // import org.joda.time._

      // import scala.util.Try

      // println("Log at top")
      // // TODO: review this
      // spark.conf.set(
      //   "spark.executorEnv.instrumentationKey",
      //   Try(spark.conf.get("spark.executorEnv.instrumentationKey")).getOrElse("dcb560a0-0ed2-460f-b67a-133eeb9b1dd2")
      // )

      // val VALIDATION_SNAPSHOT_DATETIME_FORMAT = "yyyyMMddHH"
      // val snapshotDate = DateTime.now().toString(VALIDATION_SNAPSHOT_DATETIME_FORMAT).toLong
      // val entityName: String = newState.ports.inputs(0).slug
      // // TODO: take this from arguments
      // val basePath = "dbfs:/dq/metrics/" + props.entityName

      // /*
      // spark.conf.set("dime_configstore_url", "https://dimecxdsr29sepc-fp-config-service.azurewebsites.net/api/v1/")
      // val Area = "authoring"
      // val connectionString = MetaStoreFactory.getResolver(Area).getConnectionString
      // val table = "metrics.validation_result"
      // val dbconfig = new DBConfig(table, connectionString)
      //  */

      // val rules = props.rulesJson
      // val validator = DatasetValidator(
      //   in,
      //   basePath,
      //   rules,
      //   ResultKey(snapshotDate),
      //   // dbconfig,
      //   spark,
      //   1.0,
      //   3,
      //   "lte"
      // )
      // val validateResult: ValidationResult = validator.validate()
      // if (validateResult.validationStatusCode == ValidationStatusCode.Failure) {
      //   println("Log before error")
      //   // throw new RuntimeException("Validation Failure : result oracle")
      // }

      // validateResult.result.get
      in
    }
  }
}
