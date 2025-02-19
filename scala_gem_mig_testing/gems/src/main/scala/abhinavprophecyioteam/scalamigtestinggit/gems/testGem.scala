package abhinavprophecyioteam.scalamigtestinggit.gems

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.componentSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class TestGem extends ComponentSpec {

  val name: String = "TestGem"
  val category: String = "Transform"
  type PropertiesType = TestGemProperties
  override def optimizeCode: Boolean = true

  case class TestGemProperties(
    @Property("Property1")
    property1: String = ""
  ) extends ComponentProperties

  implicit val TestGemPropertiesFormat: Format[TestGemProperties] = Json.format

  def dialog: Dialog = Dialog("TestGem")

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import org.apache.spark.sql.protobuf.functions._
    Nil
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  def deserializeProperty(props: String): TestGemProperties = Json.parse(props).as[TestGemProperties]

  def serializeProperty(props: TestGemProperties): String = Json.toJson(props).toString()

  class TestGemCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      // import org.apache.spark.sql.protobuf.functions._
      val out = in
      out
    }
  }
}
