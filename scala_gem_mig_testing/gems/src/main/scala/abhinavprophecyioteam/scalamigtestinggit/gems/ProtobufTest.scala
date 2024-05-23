package abhinavprophecyioteam.scalamigtestinggit.gems

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.datasetSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class ProtobufTest extends DatasetSpec {

  val name: String = "ProtobufTest"
  val datasetType: String = "File"

  type PropertiesType = ProtobufTestProperties
  case class ProtobufTestProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Path")
    path: String = ""
    ) extends DatasetProperties

  implicit val ProtobufTestPropertiesFormat: Format[ProtobufTestProperties] = Json.format


  def sourceDialog: DatasetDialog = DatasetDialog("ProtobufTest")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout()
                .addElement(
                  StackItem(grow = Some(1)).addElement(
                    FieldPicker(height = Some("100%"))
                  )
                )
            ),
          "auto"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )

  def targetDialog: DatasetDialog = DatasetDialog("ProtobufTest")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%")).addElement(
              StackItem(grow = Some(1)).addElement(
                FieldPicker(height = Some("100%"))
              )
            )
          ),
          "auto"
        )
        .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class ProtobufTestFormatCode(props: ProtobufTestProperties) extends ComponentCode {
    def sourceApply(spark: SparkSession): DataFrame = {
      import org.apache.spark.sql.protobuf.functions._
      var reader = spark.read.format("ProtobufTest")
      reader.load(props.path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write.format("ProtobufTest")
      writer.save(props.path)
    }
  }
  def deserializeProperty(props: String): ProtobufTestProperties = Json.parse(props).as[ProtobufTestProperties]

  def serializeProperty(props: ProtobufTestProperties): String = Json.toJson(props).toString()

}
