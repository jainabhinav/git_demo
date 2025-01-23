from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class MetadataReformat(ComponentSpec):
    name: str = "MetadataReformat"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class MetadataReformatProperties(ComponentProperties):
        schema: Optional[StructType] = StructType([])
        # metadataTableName: Optional[str] = None
        targetColumnName: Optional[str] = None
        expressionColumnName: Optional[str] = None

    def dialog(self) -> Dialog:
        return Dialog("MetadataReformat").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(
                StackLayout()
                    # .addElement(
                    # TextBox("Metadata table name").bindPlaceholder("catalog_name.db_name.table_name").bindProperty(
                    #     "metadataTableName"))
                    .addElement(
                    TextBox("Column having target column name").bindPlaceholder("target_columns").bindProperty(
                        "targetColumnName"))
                    .addElement(TextBox("Column having expression to compute target column").bindPlaceholder(
                    "target_expressions").bindProperty("expressionColumnName")),
                "5fr"
            )
        )

    def validate(self, context: WorkflowContext, component: Component[MetadataReformatProperties]) -> List[Diagnostic]:
        diagnostics = []
        # if component.properties.metadataTableName is None:
        #     diagnostics.append(Diagnostic("properties.metadataTableName", "Metadata table name cannot be blank", SeverityLevelEnum.Error))
        if component.properties.targetColumnName is None:
            diagnostics.append(Diagnostic("properties.targetColumnName", "Column containing target column names cannot be blank", SeverityLevelEnum.Error))
        if component.properties.expressionColumnName is None:
            diagnostics.append(Diagnostic("properties.expressionColumnName", "Column containing target column expressions cannot be blank", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[MetadataReformatProperties],
                 newState: Component[MetadataReformatProperties]) -> Component[
        MetadataReformatProperties]:
        newState = replace(newState, ports=replace(newState.ports, isCustomOutputSchema=True))
        return newState

    class MetadataReformatCode(ComponentCode):
        def __init__(self, newProps):
            self.props: MetadataReformat.MetadataReformatProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
            metadata = in1.select(f"{self.props.targetColumnName}", f"{self.props.expressionColumnName}").collect()
            select_cols = [expr(row[f"{self.props.expressionColumnName}"]).alias(row[f"{self.props.targetColumnName}"]) for row in metadata]
            return in0.select(*select_cols)
