from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *


class NewDatasetFormat(DatasetSpec):
    name: str = "NewDataset"
    datasetType: str = "File"
    mode: str = "batch"
    

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class NewDatasetProperties(DatasetProperties):
        schema: Optional[StructType] = None
        description: Optional[str] = ""
        path: str = ""
        uri: Optional[str] = None

    def sourceDialog(self) -> DatasetDialog:
        return (DatasetDialog("NewDataset")
        .addSection("LOCATION", TargetLocation("path"))
        .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox()
                .addElement(
                    StackLayout(height="100%")
                    .addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
                        )
                    )
                ),
                "auto"
            )
            .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
        )
        .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")))

    def targetDialog(self) -> DatasetDialog:
        return (DatasetDialog("NewDataset")
        .addSection("LOCATION", TargetLocation("path"))
        .addSection(
            "PROPERTIES",
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                ScrollBox().addElement(
                    StackLayout(height="100%").addElement(
                        StackItem(grow=1).addElement(
                            FieldPicker(height="100%")
                        )
                    )
                ),
                "auto"
            )
            .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
        ))

    def validate(self, context: WorkflowContext, component: Component) -> list:
        return []

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState

    class NewDatasetFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: NewDatasetFormat.NewDatasetProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            reader = spark.read.parquet("asd")
            return reader

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            in0.write.parquet("asd")
