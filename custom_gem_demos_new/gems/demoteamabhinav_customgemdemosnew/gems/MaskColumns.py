from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class MaskColumns(ComponentSpec):
    name: str = "MaskColumns"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class MaskColumnsProperties(ComponentProperties):
        # properties for the component with default values
        columnsToMask: List[str] = field(default_factory=list)
        maskingTechnique: Optional[str] = None
        sha2BitLength: Optional[str] = None
        newProp: SecretValue = field(default_factory=list)

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        maskingTechniqueSelectBox = SelectBox("Masking Technique") \
                                        .addOption("sha1", "abc") \
                                        .addOption("sha2", "sha2") \
                                        .addOption("hash", "hash") \
                                        .bindProperty("maskingTechnique")

        return Dialog("MaskColumns").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(StackLayout(height=("100%")).addElement(ColumnsLayout(("1rem"))
                    .addColumn(
                    SchemaColumnsDropdown("Column Names")
                        .withMultipleSelection()
                        .withSearchEnabled()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("columnsToMask")
                        .showErrorsFor("columnsToMask"),
                    "1fr"
                    )
                ).addElement(maskingTechniqueSelectBox)
                .addElement(Condition()
                        .ifEqual(
                        PropExpr("component.properties.maskingTechnique"), StringExpr("sha2")
                    ).then(TextBox("SHA 2 Bits").bindPlaceholder("256").bindProperty("sha2BitLength"))).addElement(SecretBox("Username").bindPlaceholder("username").bindProperty("newProp"))
            )
        )

    def validate(self, context: WorkflowContext, component: Component[MaskColumnsProperties]) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = []
        
        if component.properties.maskingTechnique == "sha2":
            if component.properties.sha2BitLength is not None:
                if not component.properties.sha2BitLength in ["224", "256", "384", "512"]:
                    diagnostics.append(Diagnostic("properties.sha2BitLength", "SHA 2 bit length can be one of 224, 256, 384, 512", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[MaskColumnsProperties], newState: Component[MaskColumnsProperties]) -> Component[
    MaskColumnsProperties]:
        # Handle changes in the component's state and return the new state
        # newState = replace(newState, ports=replace(newState.ports, isCustomOutputSchema=True))
        return newState

    class MaskColumnsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: MaskColumns.MaskColumnsProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import os
            import pyhocon
            final_df = in0
            dbutils.fs.ls("asd")
            for col_name in self.props.columnsToMask:
                if self.props.maskingTechnique == "abc":
                    final_df = final_df.withColumn(col_name, sha1(col_name))
                elif self.props.maskingTechnique == "sha2":
                    if self.props.sha2BitLength is not None:
                        final_df = final_df.withColumn(col_name, sha2(col_name, int(self.props.sha2BitLength)))
                    else:
                        final_df = final_df.withColumn(col_name, sha2(col_name, 256))
                elif self.props.maskingTechnique == "hash":
                    final_df = final_df.withColumn(col_name, hash(col_name))
            return final_df
