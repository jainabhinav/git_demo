from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
from datetime import datetime, timedelta

import random
import uuid
import dataclasses
import pandas as pd

class Sanitizer(ComponentSpec):
    name: str = "Sanitizer"
    category: str = "Transform"
    gemDescription: str = "Applies a sanitization technique to PII data"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class SanitizerProperties(ComponentProperties):
        schema: Optional[StructType] = StructType([])
        redactColumns: List[str] = field(default_factory=list)
        usePseudo: Optional[bool] = True

    def dialog(self) -> Dialog:
        return Dialog("Sanitizer").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(
                    StackLayout(height="100%")
                    .addElement(ColumnsLayout(("1rem"))
                    .addColumn(
                    SchemaColumnsDropdown("Column Names")
                        .withMultipleSelection()
                        .withSearchEnabled()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("redactColumns")
                        .showErrorsFor("redactColumns"),
                    "1fr"
                    )
                )
                    .addElement(Checkbox("Use a faster, pseudonymization technique (Warning: This may not be GDPR compliant)").bindProperty("usePseudo"))
                )
        )

    def validate(self, context: WorkflowContext, component: Component[SanitizerProperties]) -> List[Diagnostic]:
        diagnostics = []
        props = component.properties

        # Validate the component's state
        if len(props.redactColumns) == 0:
            diagnostics.append(
                Diagnostic("properties.redactColumns", "Select at least one column.", SeverityLevelEnum.Error))
                
        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[SanitizerProperties], newState: Component[SanitizerProperties]) -> Component[
        SanitizerProperties]:
        return newState

    class SanitizerCode(ComponentCode):
        def __init__(self, newProps):
            self.props: Anonymizer.SanitizerProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> DataFrame:
            import pandas as pd
            from presidio_analyzer import AnalyzerEngine
            from presidio_anonymizer import AnonymizerEngine
            from presidio_anonymizer.entities.engine import OperatorConfig
            analyzer: SubstituteDisabled = AnalyzerEngine()
            anonymizer: SubstituteDisabled = AnonymizerEngine()
            broadcasted_analyzer: SubstituteDisabled = spark.sparkContext.broadcast(analyzer)
            broadcasted_anonymizer: SubstituteDisabled = spark.sparkContext.broadcast(anonymizer)
            
            #skipOptimization
            def anonymize_text(text: str) -> str:
                if text is None:
                    return None

                analyzer = broadcasted_analyzer.value
                anonymizer = broadcasted_anonymizer.value
                
                analyzer_results = analyzer.analyze(text=text, language="en")
                anonymized_results = anonymizer.anonymize(
                    text=text,
                    analyzer_results=analyzer_results,
                    operators={
                        "DEFAULT": OperatorConfig("replace", {"new_value": "<ANONYMIZED>"})
                    },
                )
                return anonymized_results.text

            def anonymize_series(s: pd.Series) -> pd.Series:
                return s.apply(anonymize_text)

            anonymize_data: SubstituteDisabled = pandas_udf(anonymize_series, returnType=StringType())
            
            for columnName in self.props.redactColumns:
                in0 = in0.withColumn(
                    columnName, anonymize_data(col(columnName))
                )

            return in0