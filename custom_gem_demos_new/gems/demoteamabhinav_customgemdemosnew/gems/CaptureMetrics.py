from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import *
from prophecy.cb.ui.uispec import *


class CaptureMetrics(ComponentSpec):
    name: str = "CaptureMetrics"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class CaptureMetricsProperties(ComponentProperties):
        schema: Optional[StructType] = StructType([])
        metricsName: str = None
        captureCount: bool = False
        showData: bool = False
        showDataRows: Optional[str] = None

    def dialog(self) -> Dialog:
        return Dialog("CaptureMetrics").addElement(
            ColumnsLayout(gap=("1rem"), height="100%")
                .addColumn(Ports(), "content")
                .addColumn(StackLayout(height=("100%"))
                .addElement(TextBox("Dataframe Name").bindPlaceholder("customers_orders_dimension").bindProperty("metricsName"))
                .addElement(
                    ColumnsLayout(gap="1rem")
                    .addColumn(Checkbox("Print dataframe count").bindProperty("captureCount"), "1fr")
                    .addColumn(Checkbox("Show dataframe").bindProperty("showData"), "1fr")
                ).addElement(
                    Condition().ifEqual(PropExpr("component.properties.showData"),
                                                BooleanExpr(True))
                                       .then(TextBox("Number of rows to show").bindPlaceholder("20").bindProperty("showDataRows"))
            ).addElement(AlertBox(
                                variant="warning",
                                _children=[
                                    Markdown(
                                    "Please note below points while using this gem: \n"
                                    "(1) Show and count operations might slow down the overall spark job as these are actions. \n"
                                    "(2) Use this gem only at places where needed or optionally turn it on/off using configurations in conditional elements in Prophecy."
                                    )
                                ]
                            ))
            , "5fr"))

    def validate(self, context: WorkflowContext, component: Component[CaptureMetricsProperties]) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = []
        isInt = True
        try:
            int(component.properties.showDataRows)
        except:
            isInt = False
        if component.properties.metricsName is None:
            diagnostics.append(Diagnostic("properties.metricsName", "Dataframe name cannot be blank.", SeverityLevelEnum.Error))
        if component.properties.captureCount == False and component.properties.showData == False:
            diagnostics.append(Diagnostic("properties.captureCount", "Both capture count and show data cannot be off. Please turn at least one of them on.", SeverityLevelEnum.Error))
        if component.properties.showDataRows is not None:
            if not isInt:
                diagnostics.append(Diagnostic("properties.showDataRows", "Number of rows to show should be integer.", SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[CaptureMetricsProperties], newState: Component[CaptureMetricsProperties]) -> Component[
    CaptureMetricsProperties]:
        return newState


    class CaptureMetricsCode(ComponentCode):
        def __init__(self, newProps):
            self.props: CaptureMetrics.CaptureMetricsProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            print("Metrics capture start for: " + self.props.metricsName)
            if self.props.captureCount:
                in0_count: SubstituteDisabled = in0.count()
                print("count: " + str(in0_count))
            if self.props.showData:
                if self.props.showDataRows is not None:
                    in0.show(int(self.props.showDataRows))
                else:
                    in0.show()
            print("Metrics capture end for: " + self.props.metricsName)
            return in0
