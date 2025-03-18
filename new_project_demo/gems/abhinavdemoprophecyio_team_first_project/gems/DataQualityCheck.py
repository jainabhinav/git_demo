from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.ComponentBuilderBase import *


class DataQualityChecks(ABC):
    pass


class dataQualityCheck(ComponentSpec):
    name: str = "DataQualityCheck"
    category: str = "Transform"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class dataQualityCheckProperties(ComponentProperties):
        # properties for the component with default values
        schema: Optional[StructType] = StructType([])
        dq_check: List[DataQualityChecks] = field(default_factory=list)
        columnsSelector: List[str] = field(default_factory=list)
        activeTab: str = "dq_check_tab"
        postAction: str = "continue_execution"
        minFailedChecks: str = ""

    @dataclass(frozen=True)
    class AddCompletenessCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddRowCountCheck(DataQualityChecks):
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddDistinctCountCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsUniqueCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        hint: str = ""

    @dataclass(frozen=True)
    class AddHasDataTypeCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        dataType: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddHasMinMaxLengthCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        minLength: str = ""
        maxLength: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddHasMeanCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddHasStandardDeviationCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddHasSumCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsNonNegativeCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsPositiveCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddLookupCheck(DataQualityChecks):
        columns: List[str] = field(default_factory=list)
        lookup_list: List[str] = field(default_factory=list)
        threshold: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsGreaterThanCheckColumnToConst(DataQualityChecks):
        left_column: str = ""
        const_value: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsGreaterThanCheckColumnToColumn(DataQualityChecks):
        left_column: str = ""
        right_column: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsGreaterThanOrEqualToCheckColumnToConst(DataQualityChecks):
        left_column: str = ""
        const_value: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsGreaterThanOrEqualToCheckColumnToColumn(DataQualityChecks):
        left_column: str = ""
        right_column: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsLessThanCheckColumnToConst(DataQualityChecks):
        left_column: str = ""
        const_value: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsLessThanCheckColumnToColumn(DataQualityChecks):
        left_column: str = ""
        right_column: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsLessThanOrEqualToCheckColumnToConst(DataQualityChecks):
        left_column: str = ""
        const_value: str = ""
        hint: str = ""

    @dataclass(frozen=True)
    class AddIsLessThanOrEqualToCheckColumnToColumn(DataQualityChecks):
        left_column: str = ""
        right_column: str = ""
        hint: str = ""

    def onButtonClick(self, state: Component[dataQualityCheckProperties]):
        _dq_check = state.properties.dq_check
        _dq_check.append(self.AddCompletenessCheck())
        return state.bindProperties(replace(state.properties, dq_check=_dq_check))

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component

        selectBox = (SelectBox("Check Type")
                     .addOption("Completeness Check", "AddCompletenessCheck")
                     .addOption("Row Count Check", "AddRowCountCheck")
                     .addOption("Distinct Count Check", "AddDistinctCountCheck")
                     .addOption("Uniqueness Check", "AddIsUniqueCheck")
                     .addOption("Data Type Check", "AddHasDataTypeCheck")
                     .addOption("Min-Max Length Check", "AddHasMinMaxLengthCheck")
                     .addOption("Total Sum Check", "AddHasSumCheck")
                     .addOption("Mean Value Check", "AddHasMeanCheck")
                     .addOption("Standard Deviation Check", "AddHasStandardDeviationCheck")
                     .addOption("Non Negative Value Check", "AddIsNonNegativeCheck")
                     .addOption("Positive Value Check", "AddIsPositiveCheck")
                     .addOption("Lookup Check", "AddLookupCheck")
                     .addOption("Column To Constant Value Greater Than Check", "AddIsGreaterThanCheckColumnToConst")
                     .addOption("Column To Constant Value Greater Than Or Equal To Check",
                                "AddIsGreaterThanOrEqualToCheckColumnToConst")
                     .addOption("Column To Constant Value Less Than Check", "AddIsLessThanCheckColumnToConst")
                     .addOption("Column To Constant Value Less Than Or Equal To Check",
                                "AddIsLessThanOrEqualToCheckColumnToConst")
                     .addOption("Column To Column Greater Than Check", "AddIsGreaterThanCheckColumnToColumn")
                     .addOption("Column To Column Greater Than Or Equal To Check",
                                "AddIsGreaterThanOrEqualToCheckColumnToColumn")
                     .addOption("Column To Column Less Than Check", "AddIsLessThanCheckColumnToColumn")
                     .addOption("Column To Column Less Than Or Equal To Check",
                                "AddIsLessThanOrEqualToCheckColumnToColumn")
                     .bindProperty("record.kind"))

        add_completeness_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddCompletenessCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddCompletenessCheck.columns")
                                    .showErrorsFor("record.AddCompletenessCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Threshold In % (Optional)").bindPlaceholder("50").bindProperty(
                            "record.AddCompletenessCheck.threshold"), "1fr", overflow="visible"
                        )
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddCompletenessCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_row_count_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddRowCountCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            TextBox("Threshold Count").bindPlaceholder("10").bindProperty(
                                "record.AddRowCountCheck.threshold"),
                            "1fr",
                            overflow="visible"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddRowCountCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_distinct_count_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddDistinctCountCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddDistinctCountCheck.columns")
                                    .showErrorsFor("record.AddDistinctCountCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Threshold Distinct Count").bindPlaceholder("1").bindProperty(
                            "record.AddDistinctCountCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddDistinctCountCheck.hint"),
                                       "1fr", overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_is_unique_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsUniqueCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddIsUniqueCheck.columns")
                                    .showErrorsFor("record.AddIsUniqueCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddIsUniqueCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_has_data_type_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddHasDataTypeCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddHasDataTypeCheck.columns")
                                    .showErrorsFor("record.AddHasDataTypeCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(SelectBox("Data Type")
                                       .addOption("Boolean", "Boolean")
                                       .addOption("Fractional", "Fractional")
                                       .addOption("Integral", "Integral")
                                       .addOption("Numeric", "Numeric")
                                       .addOption("String", "String")
                                       .bindProperty("record.AddHasDataTypeCheck.dataType"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddHasDataTypeCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_has_min_max_length_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddHasMinMaxLengthCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddHasMinMaxLengthCheck.columns")
                                    .showErrorsFor("record.AddHasMinMaxLengthCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Minimum Length").bindPlaceholder("1").bindProperty(
                            "record.AddHasMinMaxLengthCheck.minLength"), "1fr", overflow="visible")
                            .addColumn(TextBox("Maximum Length").bindPlaceholder("50").bindProperty(
                            "record.AddHasMinMaxLengthCheck.maxLength"), "1fr", overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddHasMinMaxLengthCheck.hint"),
                                       "1fr", overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_has_sum_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddHasSumCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddHasSumCheck.columns")
                                    .showErrorsFor("record.AddHasSumCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Total Sum").bindPlaceholder("100000").bindProperty(
                            "record.AddHasSumCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddHasSumCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_is_non_negative_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsNonNegativeCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddIsNonNegativeCheck.columns")
                                    .showErrorsFor("record.AddIsNonNegativeCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Threshold In % (Optional)").bindPlaceholder("100").bindProperty(
                            "record.AddIsNonNegativeCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddIsNonNegativeCheck.hint"),
                                       "1fr", overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_is_positive_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsPositiveCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddIsPositiveCheck.columns")
                                    .showErrorsFor("record.AddIsPositiveCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Threshold In % (Optional)").bindPlaceholder("100").bindProperty(
                            "record.AddIsPositiveCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddIsPositiveCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_lookup_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddLookupCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddLookupCheck.columns")
                                    .showErrorsFor("record.AddLookupCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(SelectBox("List Of Lookup Values", mode="tags").bindProperty(
                            "record.AddLookupCheck.lookup_list"), "1fr", overflow="visible")
                            .addColumn(TextBox("Threshold In % (Optional)").bindPlaceholder("100").bindProperty(
                            "record.AddLookupCheck.threshold"), "1fr", overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddLookupCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_is_greater_than_checks_column_to_const = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsGreaterThanCheckColumnToConst")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsGreaterThanCheckColumnToConst.left_column")
                                        .showErrorsFor("record.AddIsGreaterThanCheckColumnToConst.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(TextBox("Value").bindPlaceholder("").bindProperty(
                                "record.AddIsGreaterThanCheckColumnToConst.const_value"), "1fr", overflow="visible")
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsGreaterThanCheckColumnToConst.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_greater_than_checks_column_to_column = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsGreaterThanCheckColumnToColumn")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Left Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsGreaterThanCheckColumnToColumn.left_column")
                                        .showErrorsFor("record.AddIsGreaterThanCheckColumnToColumn.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Right Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsGreaterThanCheckColumnToColumn.right_column")
                                        .showErrorsFor("record.AddIsGreaterThanCheckColumnToColumn.right_column")
                                ),
                                "1fr"
                            )
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsGreaterThanCheckColumnToColumn.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_greater_than_or_equal_to_checks_column_to_const = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsGreaterThanOrEqualToCheckColumnToConst")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsGreaterThanOrEqualToCheckColumnToConst.left_column")
                                        .showErrorsFor("record.AddIsGreaterThanOrEqualToCheckColumnToConst.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(TextBox("Value").bindPlaceholder("").bindProperty(
                                "record.AddIsGreaterThanOrEqualToCheckColumnToConst.const_value"), "1fr",
                                overflow="visible")
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsGreaterThanOrEqualToCheckColumnToConst.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_greater_than_or_equal_to_checks_column_to_column = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsGreaterThanOrEqualToCheckColumnToColumn")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Left Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsGreaterThanOrEqualToCheckColumnToColumn.left_column")
                                        .showErrorsFor(
                                        "record.AddIsGreaterThanOrEqualToCheckColumnToColumn.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Right Column")
                                        .bindSchema("schema")
                                        .bindProperty(
                                        "record.AddIsGreaterThanOrEqualToCheckColumnToColumn.right_column")
                                        .showErrorsFor(
                                        "record.AddIsGreaterThanOrEqualToCheckColumnToColumn.right_column")
                                ),
                                "1fr"
                            )
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsGreaterThanOrEqualToCheckColumnToColumn.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_less_than_checks_column_to_const = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsLessThanCheckColumnToConst")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanCheckColumnToConst.left_column")
                                        .showErrorsFor("record.AddIsLessThanCheckColumnToConst.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(TextBox("Value").bindPlaceholder("").bindProperty(
                                "record.AddIsLessThanCheckColumnToConst.const_value"), "1fr", overflow="visible")
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsLessThanCheckColumnToConst.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_less_than_checks_column_to_column = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsLessThanCheckColumnToColumn")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Left Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanCheckColumnToColumn.left_column")
                                        .showErrorsFor("record.AddIsLessThanCheckColumnToColumn.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Right Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanCheckColumnToColumn.right_column")
                                        .showErrorsFor("record.AddIsLessThanCheckColumnToColumn.right_column")
                                ),
                                "1fr"
                            )
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsLessThanCheckColumnToColumn.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_less_than_or_equal_to_checks_column_to_const = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsLessThanOrEqualToCheckColumnToConst")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanOrEqualToCheckColumnToConst.left_column")
                                        .showErrorsFor("record.AddIsLessThanOrEqualToCheckColumnToConst.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(TextBox("Value").bindPlaceholder("").bindProperty(
                                "record.AddIsLessThanOrEqualToCheckColumnToConst.const_value"), "1fr",
                                overflow="visible")
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsLessThanOrEqualToCheckColumnToConst.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_is_less_than_or_equal_to_checks_column_to_column = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddIsLessThanOrEqualToCheckColumnToColumn")) \
            .then(
            StackLayout()
                .addElement(
                Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                    .addElement(
                    ColumnsLayout(("1rem"), alignY=("end")) \
                        .addColumn(
                        StackLayout()
                            .addElement(
                            ColumnsLayout(gap="1rem")
                                .addColumn(selectBox, "1fr")
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Left Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanOrEqualToCheckColumnToColumn.left_column")
                                        .showErrorsFor("record.AddIsLessThanOrEqualToCheckColumnToColumn.left_column")
                                ),
                                "1fr"
                            )
                        )
                            .addElement(
                            ColumnsLayout(("1rem"))
                                .addColumn(
                                StackLayout(height=("100%"))
                                    .addElement(
                                    SchemaColumnsDropdown("Right Column")
                                        .bindSchema("schema")
                                        .bindProperty("record.AddIsLessThanOrEqualToCheckColumnToColumn.right_column")
                                        .showErrorsFor("record.AddIsLessThanOrEqualToCheckColumnToColumn.right_column")
                                ),
                                "1fr"
                            )
                                .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                                "Custom message for check failure").bindProperty(
                                "record.AddIsLessThanOrEqualToCheckColumnToColumn.hint"), "1fr", overflow="visible")
                        ),
                        "1fr",
                        overflow=("visible")
                    )
                )
            )
        )

        add_has_mean_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddHasMeanCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddHasMeanCheck.columns")
                                    .showErrorsFor("record.AddHasMeanCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Mean Value").bindPlaceholder("24.5").bindProperty(
                            "record.AddHasMeanCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty("record.AddHasMeanCheck.hint"), "1fr",
                                       overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        add_has_standard_deviation_checks = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("AddHasStandardDeviationCheck")) \
            .then(
            ColumnsLayout(("1rem"), alignY=("end")) \
                .addColumn(
                StackLayout()
                    .addElement(
                    Card(header=TitleElement(title="Check"), collapsible=True, collapsed=False)
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(selectBox, "1fr")
                            .addColumn(
                            StackLayout(height=("100%"))
                                .addElement(
                                SchemaColumnsDropdown("Columns")
                                    .withMultipleSelection()
                                    .bindSchema("schema")
                                    .bindProperty("record.AddHasStandardDeviationCheck.columns")
                                    .showErrorsFor("record.AddHasStandardDeviationCheck.columns")
                            ),
                            "1fr"
                        )
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Standard Deviation").bindPlaceholder("5.5").bindProperty(
                            "record.AddHasStandardDeviationCheck.threshold"), "1fr", overflow="visible")
                            .addColumn(TextBox("Failure Message (Optional)").bindPlaceholder(
                            "Custom message for check failure").bindProperty(
                            "record.AddHasStandardDeviationCheck.hint"), "1fr", overflow="visible")
                    )
                ),
                "1fr",
                overflow=("visible")
            )
        )

        checkRadioGroup = (RadioGroup("Post Action After Data Quality Checks")
                           .addOption("Continue Execution", "continue_execution",
                                      description=(
                                          "Select this option to continue pipeline execution irrespective of data quality checks success/failure"))
                           .addOption("Terminate Execution", "terminate_execution",
                                      description="Select this option to terminate pipeline execution based on allowed number of failures. If a data quality checks fails for a column, it is considered as one failed check."
                                      )
                           .setOptionType("button")
                           .setVariant("medium")
                           .setButtonStyle("solid")
                           .bindProperty("postAction")
                           )

        settingsTab = StackLayout(height=("100bh")) \
            .addElement(checkRadioGroup) \
            .addElement(
            Condition()
                .ifEqual(PropExpr("component.properties.postAction"), StringExpr("terminate_execution"))
                .then(
                TextBox("Maximum Number Of Allowed Failed Checks").bindPlaceholder("1").bindProperty("minFailedChecks")
            )
        )

        checksTab = StackLayout(gap=("1rem"), height=("100bh")) \
            .addElement(
            OrderedList("Data Quality Checks")
                .bindProperty("dq_check")
                .setEmptyContainerText("Add Checks")
                .addElement(
                add_completeness_checks
            )
                .addElement(
                add_row_count_checks
            )
                .addElement(
                add_distinct_count_checks
            )
                .addElement(
                add_is_unique_checks
            )
                .addElement(
                add_has_data_type_checks
            )
                .addElement(
                add_has_min_max_length_checks
            )
                .addElement(
                add_has_sum_checks
            )
                .addElement(
                add_is_non_negative_checks
            )
                .addElement(
                add_is_positive_checks
            )
                .addElement(
                add_lookup_checks
            )
                .addElement(
                add_is_greater_than_checks_column_to_const
            )
                .addElement(
                add_is_greater_than_checks_column_to_column
            )
                .addElement(
                add_is_greater_than_or_equal_to_checks_column_to_const
            )
                .addElement(
                add_is_greater_than_or_equal_to_checks_column_to_column
            )
                .addElement(
                add_is_less_than_checks_column_to_const
            )
                .addElement(
                add_is_less_than_checks_column_to_column
            )
                .addElement(
                add_is_less_than_or_equal_to_checks_column_to_const
            )
                .addElement(
                add_is_less_than_or_equal_to_checks_column_to_column
            )
                .addElement(
                add_has_mean_checks
            )
                .addElement(
                add_has_standard_deviation_checks
            )
        ) \
            .addElement(SimpleButtonLayout("Add Checks", self.onButtonClick))
        tabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(
            TabPane("Data Quality Checks", "dq_check_tab").addElement(checksTab)
        ) \
            .addTabPane(
            TabPane("Post Actions", "settings").addElement(settingsTab)
        )

        alertBox = StackLayout(gap=("1rem"), height=("100%")).addElement(
            AlertBox(
                variant="info",
                _children=[
                    Markdown(
                        "This Gem has the following requirement(s): (1) not supported on Unity Catalog Clusters, and (2) the following dependencies must be manually installed on the Spark cluster: [pydeequ](https://pypi.org/project/pydeequ/) (PyPi) and [deequ](https://mvnrepository.com/artifact/com.amazon.deequ/deequ) (Maven)"
                    )
                ]
            )
        ).addElement(
            tabs
        )
        return Dialog("Data Quality Check") \
            .addElement(
            ColumnsLayout(height=("100%"))
                .addColumn(PortSchemaTabs(selectedFieldsProperty=("columnsSelector")).importSchema(), "2fr")
                .addColumn(VerticalDivider(), width="content")
                .addColumn(alertBox, "5fr")
        )

    def validate(self, context: WorkflowContext, component: Component[dataQualityCheckProperties]) -> List[Diagnostic]:
        diagnostics = []

        if component.properties.postAction == "terminate_execution" and (
                component.properties.minFailedChecks is None or component.properties.minFailedChecks == ""):
            diagnostics.append(
                Diagnostic("properties.minFailedChecks", "Minimum numbers of failed check can not be empty",
                           SeverityLevelEnum.Error))

        for checks in component.properties.dq_check:
            if isinstance(checks, dataQualityCheck().AddCompletenessCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddCompletenessCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddRowCountCheck):
                if checks.threshold is None or checks.threshold == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddRowCountCheck : Row count threshold can not be empty, provide some integer value",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddDistinctCountCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddDistinctCountCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.threshold is None or checks.threshold == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddDistinctCountCheck : Distinct count threshold can not be empty, provide some integer value",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsUniqueCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsUniqueCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddHasDataTypeCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasDataTypeCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.dataType is None or checks.dataType == "":
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasDataTypeCheck : Data type can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddHasMinMaxLengthCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasMinMaxLengthCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.minLength is None or checks.minLength == "" or checks.minLength is None or checks.maxLength == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddHasMinMaxLengthCheck : Min & Max length can not be empty, provide some integer value",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddHasSumCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasSumCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.threshold is None or checks.threshold == "":
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasSumCheck : Total sum value can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsNonNegativeCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsNonNegativeCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsPositiveCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsPositiveCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddLookupCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddLookupCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.lookup_list is None or len(checks.lookup_list) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddLookupCheck : Lookup list can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsGreaterThanCheckColumnToConst):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanCheckColumnToConst : Column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.const_value is None or checks.const_value == "":
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsGreaterThanCheckColumnToConst : Value can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsGreaterThanCheckColumnToColumn):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanCheckColumnToColumn : Left column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.right_column is None or len(checks.right_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanCheckColumnToColumn : Right column can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsGreaterThanOrEqualToCheckColumnToConst):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanOrEqualToCheckColumnToConst : Column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.const_value is None or checks.const_value == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanOrEqualToCheckColumnToConst : Value can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsGreaterThanOrEqualToCheckColumnToColumn):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanOrEqualToCheckColumnToColumn : Left column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.right_column is None or len(checks.right_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsGreaterThanOrEqualToCheckColumnToColumn : Right column can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsLessThanCheckColumnToConst):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsLessThanCheckColumnToConst : Column can not be empty",
                                   SeverityLevelEnum.Error))

                if checks.const_value is None or checks.const_value == "":
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddIsLessThanCheckColumnToConst : Value can not be empty",
                                   SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsLessThanCheckColumnToColumn):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanCheckColumnToColumn : Left column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.right_column is None or len(checks.right_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanCheckColumnToColumn : Right column can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsLessThanOrEqualToCheckColumnToConst):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanOrEqualToCheckColumnToConst : Column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.const_value is None or checks.const_value == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanOrEqualToCheckColumnToConst : Value can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddIsLessThanOrEqualToCheckColumnToColumn):
                if checks.left_column is None or len(checks.left_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanOrEqualToCheckColumnToColumn : Left column can not be empty",
                                                  SeverityLevelEnum.Error))

                if checks.right_column is None or len(checks.right_column) == 0:
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddIsLessThanOrEqualToCheckColumnToColumn : Right column can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddHasMeanCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasMeanCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.threshold is None or checks.threshold == "":
                    diagnostics.append(Diagnostic("properties.dq_check", "AddHasSumCheck : Mean value can not be empty",
                                                  SeverityLevelEnum.Error))
            elif isinstance(checks, dataQualityCheck().AddHasStandardDeviationCheck):
                if checks.columns is None or len(checks.columns) == 0:
                    diagnostics.append(
                        Diagnostic("properties.dq_check", "AddHasStandardDeviationCheck : Column list can not be empty",
                                   SeverityLevelEnum.Error))
                if checks.threshold is None or checks.threshold == "":
                    diagnostics.append(Diagnostic("properties.dq_check",
                                                  "AddHasStandardDeviationCheck : Standard deviation value can not be empty",
                                                  SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component[dataQualityCheckProperties],
                 newState: Component[dataQualityCheckProperties]) -> Component[
        dataQualityCheckProperties]:
        newSchema = newState.ports.inputs[0].schema
        if newSchema is None:
            newSchema = StructType([])
        newProperties = replace(replace(newState.properties, schema=newSchema))

        newState = newState.bindProperties(newProperties)
        newState = replace(newState, ports=replace(newState.ports, isCustomOutputSchema=True))
        return newState

    class dataQualityCheckCode(ComponentCode):
        def __init__(self, newProps):
            self.props: dataQualityCheck.dataQualityCheckProperties = newProps

        def apply(self, spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
            import re
            from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
            from pydeequ.verification import VerificationSuite, VerificationResult

            checkObj = Check(spark, CheckLevel.Warning, "Data Quality Checks")
            tmp_df = in0

            for checks in self.props.dq_check:
                if isinstance(checks, dataQualityCheck().AddCompletenessCheck):
                    threshold = float(
                        checks.threshold) / 100 if checks.threshold is not None and checks.threshold != "" else 1.0

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"{threshold * 100}% values should be non-null for {column}"
                        checkObj = checkObj.hasCompleteness(column, lambda x: x >= threshold, hint=msg)

                elif isinstance(checks, dataQualityCheck().AddRowCountCheck):
                    threshold = int(checks.threshold)
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"The number of rows should be at least {threshold}"

                    checkObj = checkObj.hasSize(lambda x: x >= threshold, hint=msg)

                elif isinstance(checks, dataQualityCheck().AddDistinctCountCheck):
                    threshold = int(checks.threshold)

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} should have {threshold} distinct values"
                        checkObj = checkObj.hasNumberOfDistinctValues(column, lambda x: x == threshold, None, None,
                                                                      hint=msg)

                elif isinstance(checks, dataQualityCheck().AddIsUniqueCheck):
                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} is not unique"
                        checkObj = checkObj.isUnique(column, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddHasDataTypeCheck):
                    data_type_dict = {
                        "Boolean": ConstrainableDataTypes.Boolean,
                        "Fractional": ConstrainableDataTypes.Fractional,
                        "Integral": ConstrainableDataTypes.Integral,
                        "Numeric": ConstrainableDataTypes.Numeric,
                        "String": ConstrainableDataTypes.String
                    }
                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} is not of {checks.dataType} data type"
                        checkObj = checkObj.hasDataType(column, data_type_dict[checks.dataType], lambda x: x == True,
                                                        hint=msg)
                elif isinstance(checks, dataQualityCheck().AddHasMinMaxLengthCheck):
                    minLength = int(checks.minLength)
                    maxLength = int(checks.maxLength)

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Length of column {column} does not lie between {minLength} and {maxLength}"
                        checkObj = checkObj.hasMaxLength(column, lambda x: x >= minLength and x <= maxLength, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddHasSumCheck):
                    threshold = float(checks.threshold)

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Total sum of column {column} is not equals to {threshold}"
                        checkObj = checkObj.hasSum(column, lambda x: x == threshold, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsNonNegativeCheck):
                    threshold = float(
                        checks.threshold) / 100 if checks.threshold is not None and checks.threshold != "" else 1.0

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} should have (atleast) {threshold * 100}% non negative values"
                        checkObj = checkObj.isNonNegative(column, lambda x: x >= threshold, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsPositiveCheck):
                    threshold = float(
                        checks.threshold) / 100 if checks.threshold is not None and checks.threshold != "" else 1.0

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} should have (atleast) {threshold * 100}% positive values"
                        checkObj = checkObj.isPositive(column, lambda x: x >= threshold, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddLookupCheck):
                    threshold = float(
                        checks.threshold) / 100 if checks.threshold is not None and checks.threshold != "" else 1.0
                    lookup_list = checks.lookup_list

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {column} should have (atleast) {threshold * 100}% values lie in {lookup_list}"
                        checkObj = checkObj.isContainedIn(column, lookup_list, lambda x: x >= threshold, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsGreaterThanCheckColumnToConst):
                    left_col = checks.left_column
                    const_val = checks.const_value
                    tmp_df = tmp_df.withColumn(left_col + "_gt_const", lit(const_val))
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not greater than value {const_val}"
                    checkObj = checkObj.isGreaterThan(left_col, left_col + "_gt_const", lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsGreaterThanCheckColumnToColumn):
                    left_col = checks.left_column
                    right_col = checks.right_column
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not greater than {right_col}"
                    checkObj = checkObj.isGreaterThan(left_col, right_col, lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsGreaterThanOrEqualToCheckColumnToConst):
                    left_col = checks.left_column
                    const_val = checks.const_value
                    tmp_df = tmp_df.withColumn(left_col + "_gt_const", lit(const_val))
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not greater than or equal to value {const_val}"
                    checkObj = checkObj.isGreaterThanOrEqualTo(left_col, left_col + "_gt_const", lambda x: x == True,
                                                               hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsGreaterThanOrEqualToCheckColumnToColumn):
                    left_col = checks.left_column
                    right_col = checks.right_column
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not greater than or equal to value {right_col}"
                    checkObj = checkObj.isGreaterThanOrEqualTo(left_col, right_col, lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsLessThanCheckColumnToConst):
                    left_col = checks.left_column
                    const_val = checks.const_value
                    tmp_df = tmp_df.withColumn(left_col + "_lt_const", lit(const_val))
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not less than value {const_val}"
                    checkObj = checkObj.isLessThan(left_col, left_col + "_lt_const", lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsLessThanCheckColumnToColumn):
                    left_col = checks.left_column
                    right_col = checks.right_column
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not less than {right_col}"
                    checkObj = checkObj.isLessThan(left_col, right_col, lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsLessThanOrEqualToCheckColumnToConst):
                    left_col = checks.left_column
                    const_val = checks.const_value
                    tmp_df = tmp_df.withColumn(left_col + "_lte_const", lit(const_val))
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not less than or equal to value {const_val}"
                    checkObj = checkObj.isLessThanOrEqualTo(left_col, left_col + "_lte_const", lambda x: x == True,
                                                            hint=msg)
                elif isinstance(checks, dataQualityCheck().AddIsLessThanOrEqualToCheckColumnToColumn):
                    left_col = checks.left_column
                    right_col = checks.right_column
                    msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Column {left_col} is not less than or equal to value {right_col}"
                    checkObj = checkObj.isLessThanOrEqualTo(left_col, right_col, lambda x: x == True, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddHasMeanCheck):
                    threshold = float(checks.threshold)

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Mean value of column {column} is not equals to {threshold}"
                        checkObj = checkObj.hasMean(column, lambda x: x == threshold, hint=msg)
                elif isinstance(checks, dataQualityCheck().AddHasStandardDeviationCheck):
                    threshold = float(checks.threshold)

                    for column in checks.columns:
                        msg = checks.hint if checks.hint is not None and checks.hint != "" else f"Standard deviation of column {column} is not equals to {threshold}"
                        checkObj = checkObj.hasStandardDeviation(column, lambda x: x == threshold, hint=msg)

            checkResult = VerificationSuite(spark) \
                .onData(tmp_df) \
                .addCheck(
                checkObj
            ) \
                .run()

            checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
            df = checkResult_df.selectExpr("constraint_status", "constraint_message",
                                           "udf_extract_check_and_column(constraint) as parsed")
            result_df = df.selectExpr(
                "parsed._1 as check_type",
                "parsed._2 as column",
                "constraint_status",
                "constraint_message"
            )

            if self.props.postAction == "terminate_execution":
                failed_checks_count: SubstituteDisabled = result_df.filter(
                    col('constraint_status') != 'Success').count()

                if failed_checks_count > int(self.props.minFailedChecks):
                    print(f"Data quality check failed: {failed_checks_count} checks did not pass.")
                    raise Exception(f"Data quality check failed: {failed_checks_count} checks did not pass.")

            return in0, result_df
