from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *


class jdbc_update(ComponentSpec):
    name: str = "jdbc_update"
    category: str = "Custom"

    def optimizeCode(self) -> bool:
        # Return whether code optimization is enabled for this component
        return True

    @dataclass(frozen=True)
    class jdbc_updateProperties(ComponentProperties):
        # properties for the component with default values
        secretUsername: SecretValue = field(default_factory=list)
        secretPassword: SecretValue = field(default_factory=list)
        secretJdbcUrl: SecretValue = field(default_factory=list)
        updateStatement: SString = SString("UPDATE ....")

    def dialog(self) -> Dialog:
        # Define the UI dialog structure for the component
        return Dialog("jdbc_update").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                #            .addElement(TitleElement(title = "Credentials"))
                .addElement(
                    StackLayout()
                    .addElement(
                        ColumnsLayout(gap="1rem")
                        .addColumn(SecretBox("Username").bindPlaceholder("username").bindProperty("secretUsername"))
                        .addColumn(SecretBox("Password").isPassword().bindPlaceholder("password").bindProperty("secretPassword"))
                    )
                )
                .addElement(TitleElement(title="URL"))
                .addElement(
                    SecretBox("JDBC URL")
                    .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                    .bindProperty("secretJdbcUrl")
                )
                .addElement(TitleElement(title="Update Statement"))
                .addElement(
                    TextBox("Update Statement")
                    .bindPlaceholder("UPDATE ...")
                    .bindProperty("updateStatement")
                )
            )
        )

    def validate(self, context: WorkflowContext, component: Component[jdbc_updateProperties]) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: WorkflowContext, oldState: Component[jdbc_updateProperties], newState: Component[jdbc_updateProperties]) -> Component[
    jdbc_updateProperties]:
        # Handle changes in the component's state and return the new state
        newState = replace(newState, ports=replace(newState.ports, isCustomOutputSchema=True))
        return newState


    class jdbc_updateCode(ComponentCode):
        def __init__(self, newProps):
            self.props: jdbc_update.jdbc_updateProperties = newProps


        def apply(self, spark: SparkSession):
            # This method contains logic used to generate the spark code from the given inputs.

            import pyspark.dbutils

            sql_driver_manager=spark._sc._gateway.jvm.java.sql.DriverManager
            con = sql_driver_manager.getConnection(self.props.secretJdbcUrl, self.props.secretUsername, self.props.secretPassword)

            con.prepareCall(updateStatement).execute()
            con.close()