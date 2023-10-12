from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


@dataclass(frozen=True)
class MaskingRule:
    colName: str
    maskingType: str
    extraParams: Optional[str] = None

class Masking(ComponentSpec):
    name: str = "Masking"
    category: str = "Transform"
    gemDescription: str = "Masks the columns based on user input"
    docUrl: str = ""

    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class MaskingProperties(ComponentProperties):
        columnsSelector: List[str] = field(default_factory=list)
        rulesColumns: List[MaskingRule] = field(default_factory=list)

    def onClickFunc(
        self, portId: str, column: str, state: Component[MaskingProperties]
    ):
        rules = state.properties.rulesColumns
        rules.append(MaskingRule(column, "sha1"))
        return state.bindProperties(replace(state.properties, rulesColumns=rules))

    def dialog(self) -> Dialog:
        selectBox = (
            SelectBox("")
                .addOption("MD5", "md5")
                .addOption("Sha1", "sha1")
                .addOption("Sha2", "sha2")
                .addOption("Sha3", "sha3")
        )

        ruleTable = BasicTable(
            "Rule Table",
            height="300px",
            columns=[
                Column(
                    "Column Name",
                    "colName",
                    (TextBox("", ignoreTitle=True).disabled().bindPlaceholder("column_name")),
                ),
                Column(
                    "Masking Strategy",
                    "maskingType",
                    selectBox,
                ),
                Column(
                    "Extra Argument",
                    "extraParams",
                    (TextBox("", ignoreTitle=True).bindPlaceholder("num_bits")),
                )
            ],
        ).bindProperty("rulesColumns")
        
        return Dialog("Masking").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                PortSchemaTabs(
                    selectedFieldsProperty=("columnsSelector"),
                    singleColumnClickCallback=self.onClickFunc,
                ).importSchema()
            ).addColumn(StackLayout(height=("100%"))
                    .addElement(ruleTable), "2fr")
        )

    def validate(self, context: WorkflowContext, component: Component[MaskingProperties]) -> List[Diagnostic]:
        diagnostics = []

        if len(component.properties.rulesColumns) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.rulesColumns",
                    "At least one masking rule has to be specified",
                    SeverityLevelEnum.Error,
                )
            )
        
        for idx, rule in enumerate(component.properties.rulesColumns):
            if rule.maskingType == "sha2":
                if rule.extraParams is None:
                    diagnostics.append(
                        Diagnostic(
                            f"properties.rulesColumns[{idx}].extraParams",
                            "Extra Argument needs to be provided for sha2. Please provide num bits. Values can be 224, 256, 384, 512, or 0 (which is equivalent to 256).",
                            SeverityLevelEnum.Error
                        )
                    )
                
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[MaskingProperties], newState: Component[MaskingProperties]) -> Component[
    MaskingProperties]:
        
        return newState


    class MaskingCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Masking.MaskingProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            df = in0
            for rules in self.props.rulesColumns:
                if rules.maskingType == "md5":
                    df = df.withColumn(rules.colName + "_" + rules.maskingType , md5(col(rules.colName)))
                elif rules.maskingType == "sha1":
                    df = df.withColumn(rules.colName + "_" + rules.maskingType , sha1(col(rules.colName)))
                elif rules.maskingType == "sha2":
                    df = df.withColumn(rules.colName + "_" + rules.maskingType , sha2(col(rules.colName), int(rules.extraParams)))
                elif rules.maskingType == "sha3":
                    df = df.withColumn(rules.colName + "_" + rules.maskingType , sha2(col(rules.colName), int(rules.extraParams)))
            return df
