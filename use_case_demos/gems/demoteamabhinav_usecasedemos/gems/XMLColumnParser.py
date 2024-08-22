from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
import dataclasses


class XMLColumnParser(ComponentSpec):
    name: str = "XMLColumnParser"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class XMLColumnParserProperties(ComponentProperties):
        xmlColumnToParse: str = ""
        parsingMethod: str = "parseAuto"
        sampleRecord: Optional[str] = None
        xmlSchema: Optional[str] = None

    def dialog(self) -> Dialog:
        methodRadioGroup = RadioGroup("Parsing method") \
            .addOption("Parse automatically", "parseAuto", description=(
            "Parse automatically from sample record in dataframe")) \
            .addOption("Parse from sample record", "parseFromSampleRecord", description=(
            "provide a sample record to parse the data from")) \
            .addOption("Parse from schema", "parseFromSchema",
                       description=("Provide sample schema in SQL format to parse the data with")) \
            .setOptionType("button") \
            .setVariant("medium") \
            .setButtonStyle("solid") \
            .bindProperty("parsingMethod")


        return Dialog("XMLColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(
                StackLayout(height=("100%"))
                    .addElement(ColumnsLayout(("1rem"))
                    .addColumn(
                    SchemaColumnsDropdown("Source Column Name")
                        .withSearchEnabled()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("xmlColumnToParse")
                        .showErrorsFor("xmlColumnToParse"),
                    "1fr"
                ))
                    .addElement(methodRadioGroup)
                .addElement(Condition()
                    .ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSampleRecord"))
                    .then(
                    TextArea("Sample XML record to parse schema from", 20).bindProperty("sampleRecord").bindPlaceholder("""
<root>
  <person>
    <id>1</id>
    <name>
      <first>John</first>
      <last>Doe</last>
    </name>
    <address>
      <street>Main St</street>
      <city>Springfield</city>
      <zip>12345</zip>
    </address>
  </person>
</root>""")))
                    .addElement(
                    Condition()
                        .ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSchema"))
                        .then(
                        TextArea("XML schema struct to parse the column", 20).bindProperty("xmlSchema").bindPlaceholder("""
STRUCT<
  person: STRUCT<
    id: INT,
    name: STRUCT<
      first: STRING,
      last: STRING
    >,
    address: STRUCT<
      street: STRING,
      city: STRING,
      zip: STRING
    >
  >
>""")
                    )
                ),
                "1fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[XMLColumnParserProperties]) -> List[Diagnostic]:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[XMLColumnParserProperties],
                 newState: Component[XMLColumnParserProperties]) -> Component[
        XMLColumnParserProperties]:
        updatedPorts = dataclasses.replace(newState.ports, inputs=newState.ports.inputs, isCustomOutputSchema=True,
                                           autoUpdateOnRun=True)
        return dataclasses.replace(newState, ports=updatedPorts)

    class XMLColumnParserCode(ComponentCode):
        def __init__(self, newProps):
            self.props: XMLColumnParser.XMLColumnParserProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            column_to_parse = self.props.xmlColumnToParse
            if self.props.parsingMethod == "parseFromSampleRecord" or self.props.parsingMethod == "parseAuto":

                def infer_schema(element):
                    if len(element) == 0:
                        return "STRING"

                    fields = []
                    for child in element:
                        field_name = child.tag
                        field_type = infer_schema(child)
                        fields.append(f"{field_name}: {field_type}")

                    return f"STRUCT<{', '.join(fields)}>"

                import xml.etree.ElementTree as ET

                if self.props.parsingMethod == "parseFromSampleRecord":
                    sample_xml = self.props.sampleRecord
                else:
                    sample_xml = in0.select(column_to_parse).take(1)[0][0]
                root = ET.fromstring(sample_xml)


                xml_schema = infer_schema(root)
                output_df = in0.withColumn("xml_parsed_content", expr(f"from_xml({column_to_parse}, '{xml_schema}')"))

            else:
                xml_schema = self.props.xmlSchema
                output_df = in0.withColumn("xml_parsed_content", expr(f"from_xml({column_to_parse}, '{xml_schema}')"))
            return output_df
