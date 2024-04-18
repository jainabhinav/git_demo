package abhinavprophecyioteam.scalamigtestinggit.gems

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.componentSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class RenameColumns extends ComponentSpec {

  val name: String = "RenameColumns"
  val category: String = "Transform"
  type PropertiesType = RenameColumnsProperties
  override def optimizeCode: Boolean = true

  case class RenameColumnsProperties(
    @Property("Property1")
    property1: String = ""
  ) extends ComponentProperties

  implicit val RenameColumnsPropertiesFormat: Format[RenameColumnsProperties] = Json.format

  def dialog: Dialog = Dialog("RenameColumns")

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  def deserializeProperty(props: String): RenameColumnsProperties = Json.parse(props).as[RenameColumnsProperties]

  def serializeProperty(props: RenameColumnsProperties): String = Json.toJson(props).toString()

  class RenameColumnsCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = in
      out
    }
  }
}
