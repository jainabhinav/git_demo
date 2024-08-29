from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.ui.uispec import *
from datetime import datetime
import dataclasses

class SyntheticDataGenerator(ABC):
    pass


class SyntheticDataGeneratorFormat(DatasetSpec):
    name: str = "SyntheticDataGenerator"
    datasetType: str = "File"
    mode: str = "batch"


    def optimizeCode(self) -> bool:
        return True

    @dataclass(frozen=True)
    class SyntheticDataGeneratorProperties(DatasetProperties):
        schema: Optional[StructType] = None
        dataGenElements: List[SyntheticDataGenerator] = field(default_factory=list)
        description: Optional[str] = ""
        rowCount: str = "10"
        path: str = ""
        activeTab: str = "random_data_gen"
        jsonSchema: str = ""
        languageAndCountry: str = "en_US"

    @dataclass(frozen=True)
    class RandomNameGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        subType: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomEmailGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomAddressGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomPhoneNumberGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        phoneNumPattern: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomIntGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        startValue: str = ""
        endValue: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomFloatGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        startValue: str = ""
        endValue: str = ""
        decimalPlaces: str = "2"
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomStringIDGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomElementGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        randomValueList: List[str] = field(default_factory=list)
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomDateGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        startDate: str = ""
        endDate: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomDateTimeGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        startDateTime: str = ""
        endDateTime: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomBooleanGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        nullCount: str = ""

    @dataclass(frozen=True)
    class RandomForeignKeyGenerator(SyntheticDataGenerator):
        columnName: str = ""
        dataType: str = ""
        refTable: str = ""
        refColumn: str = ""
        nullCount: str = ""

    def onButtonClick(self, state: Component[SyntheticDataGeneratorProperties]):
        _data_gen = state.properties.dataGenElements
        _data_gen.append(self.RandomNameGenerator())
        return state.bindProperties(dataclasses.replace(state.properties, dataGenElements=_data_gen))

    def ui_components_to_json_schema(self, state: Component[SyntheticDataGeneratorProperties]) -> Component[SyntheticDataGeneratorProperties]:
        import json
        jsonSchema = {
            "type" : "struct",
            "fields" : []
        }

        for dataGenObj in state.properties.dataGenElements:
            field = {}
            if isinstance(dataGenObj, self.RandomNameGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                subType = dataGenObj.subType
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Name",
                    "sub_type" : subType
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomAddressGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Address"
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomEmailGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Email"
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomPhoneNumberGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                phoneNumPattern = dataGenObj.phoneNumPattern
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Phone Number",
                    "pattern": phoneNumPattern
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomStringIDGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random String UUID"
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomBooleanGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Boolean Values"
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomIntGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                startValue = dataGenObj.startValue
                endValue = dataGenObj.endValue
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Integer Numbers",
                    "start_value": int(startValue),
                    "end_value": int(endValue)
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomFloatGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                startValue = dataGenObj.startValue
                endValue = dataGenObj.endValue
                decimalPlaces = int(dataGenObj.decimalPlaces) if dataGenObj.decimalPlaces is not None and dataGenObj.decimalPlaces != "" else 2
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Decimal Numbers",
                    "start_value": float(startValue),
                    "end_value": float(endValue),
                    "decimal_places": decimalPlaces
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomElementGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                listValues = dataGenObj.randomValueList
                if dataGenObj.dataType == "Integer":
                    listValues = list(map(int, dataGenObj.randomValueList))
                elif dataGenObj.dataType == "Float":
                    listValues = list(map(float, dataGenObj.randomValueList))
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Element From List",
                    "elements": listValues
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomDateGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                startDate = dataGenObj.startDate
                endDate = dataGenObj.endDate
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Date",
                    "start_date": startDate,
                    "end_date": endDate
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomDateTimeGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                startDatetime = dataGenObj.startDateTime
                endDatetime = dataGenObj.endDateTime
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random DateTime",
                    "start_datetime": startDatetime,
                    "end_datetime": endDatetime
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)
            elif isinstance(dataGenObj, self.RandomForeignKeyGenerator):
                colName = dataGenObj.columnName
                dataType = dataGenObj.dataType.lower()
                refTable = dataGenObj.refTable
                refColumn = dataGenObj.refColumn
                nullPercentage = dataGenObj.nullCount
                field["name"] = colName
                field["type"] = dataType
                field["metadata"] = {
                    "provider" : "Random Foreign Key Values",
                    "ref_table": refTable,
                    "ref_column": refColumn
                }
                if nullPercentage is not None and nullPercentage != "":
                    field["metadata"]["null_percentage"] = float(nullPercentage)


            jsonSchema["fields"].append(field)

        return state.bindProperties(dataclasses.replace(state.properties, jsonSchema=json.dumps(jsonSchema, indent=4)))


    def json_schema_to_ui_components(self, state: Component[SyntheticDataGeneratorProperties]) -> Component[SyntheticDataGeneratorProperties]:
        import json
        jsonSchema = json.loads(state.properties.jsonSchema)
        dataGeneratorElements = []
        if not jsonSchema:
            jsonSchema = {}

        for fields in jsonSchema.get("fields",[]):
            colName = fields.get("name","")
            dataType = fields.get("type","").capitalize()
            provider = fields["metadata"].get("provider","")
            nullPercentage = str(fields["metadata"].get("null_percentage",""))
            if provider == "Random Name":
                subType = fields["metadata"].get("sub_type","")
                dataGeneratorElements.append(self.RandomNameGenerator(colName, dataType, subType, nullPercentage))
            elif provider == "Random Address":
                dataGeneratorElements.append(self.RandomAddressGenerator(colName, dataType, nullPercentage))
            elif provider == "Random Email":
                dataGeneratorElements.append(self.RandomEmailGenerator(colName, dataType, nullPercentage))
            elif provider == "Random Phone Number":
                pattern = str(fields["metadata"].get("pattern","###-###-####"))
                dataGeneratorElements.append(self.RandomPhoneNumberGenerator(colName, dataType, pattern, nullPercentage))
            elif provider == "Random String UUID":
                dataGeneratorElements.append(self.RandomStringIDGenerator(colName, dataType, nullPercentage))
            elif provider == "Random Boolean Values":
                dataGeneratorElements.append(self.RandomBooleanGenerator(colName, dataType, nullPercentage))
            elif provider == "Random Integer Numbers":
                start_value = str(fields["metadata"].get("start_value",""))
                end_value = str(fields["metadata"].get("end_value",""))
                dataGeneratorElements.append(self.RandomIntGenerator(colName, dataType, start_value, end_value, nullPercentage))
            elif provider == "Random Decimal Numbers":
                start_value = str(fields["metadata"].get("start_value",""))
                end_value = str(fields["metadata"].get("end_value",""))
                decimal_places = str(fields["metadata"].get("decimal_places","2"))
                dataGeneratorElements.append(self.RandomFloatGenerator(colName, dataType, start_value, end_value, decimal_places, nullPercentage))
            elif provider == "Random Element From List":
                elements = list(map(str, fields["metadata"].get("elements",[])))
                dataGeneratorElements.append(self.RandomElementGenerator(colName, dataType, elements, nullPercentage))
            elif provider == "Random Date":
                start_date = str(fields["metadata"].get("start_date",""))
                end_date = str(fields["metadata"].get("end_date",""))
                dataGeneratorElements.append(self.RandomDateGenerator(colName, dataType, start_date, end_date, nullPercentage))
            elif provider == "Random DateTime":
                start_datetime = str(fields["metadata"].get("start_datetime",""))
                end_datetime = str(fields["metadata"].get("end_datetime",""))
                dataGeneratorElements.append(self.RandomDateTimeGenerator(colName, dataType, start_datetime, end_datetime, nullPercentage))
            elif provider == "Random Foreign Key Values":
                ref_table = str(fields["metadata"].get("ref_table",""))
                ref_column = str(fields["metadata"].get("ref_column"))
                dataGeneratorElements.append(self.RandomForeignKeyGenerator(colName, dataType, ref_table, ref_column, nullPercentage))

        return state.bindProperties(dataclasses.replace(state.properties, dataGenElements=dataGeneratorElements))

    def sourceDialog(self) -> DatasetDialog:
        providers = (SelectBox("Provider")
                     .addOption("Random Name", "RandomNameGenerator")
                     .addOption("Random Address", "RandomAddressGenerator")
                     .addOption("Random Email", "RandomEmailGenerator")
                     .addOption("Random Phone Number", "RandomPhoneNumberGenerator")
                     .addOption("Random String UUID", "RandomStringIDGenerator")
                     .addOption("Random Boolean Values", "RandomBooleanGenerator")
                     .addOption("Random Integer Numbers", "RandomIntGenerator")
                     .addOption("Random Decimal Numbers", "RandomFloatGenerator")
                     .addOption("Random Element From List", "RandomElementGenerator")
                     .addOption("Random Date", "RandomDateGenerator")
                     .addOption("Random DateTime", "RandomDateTimeGenerator")
                     .addOption("Random Foreign Key Values", "RandomForeignKeyGenerator")
                     .bindProperty("record.kind"))

        languageAndCountry = (SelectBox("Language & Country")
                              .addOption("Arabic (World)", "ar_AA")
                              .addOption("Arabic (Egypt)", "ar_EG")
                              .addOption("Arabic (Jordan)", "ar_JO")
                              .addOption("Arabic (Palestine)", "ar_PS")
                              .addOption("Arabic (Saudi Arabia)", "ar_SA")
                              .addOption("Bulgarian (Bulgaria)", "bg_BG")
                              .addOption("Bosnian (Bosnia and Herzegovina)", "bs_BA")
                              .addOption("Czech (Czech Republic)", "cs_CZ")
                              .addOption("German (Austria)", "de_AT")
                              .addOption("German (Switzerland)", "de_CH")
                              .addOption("German (Germany)", "de_DE")
                              .addOption("Danish (Denmark)", "dk_DK")
                              .addOption("Greek (Greece)", "el_GR")
                              .addOption("English (Australia)", "en_AU")
                              .addOption("English (Canada)", "en_CA")
                              .addOption("English (United Kingdom)", "en_GB")
                              .addOption("English (Ireland)", "en_IE")
                              .addOption("English (India)", "en_IN")
                              .addOption("English (New Zealand)", "en_NZ")
                              .addOption("English (Philippines)", "en_PH")
                              .addOption("English (Singapore)", "en_SG")
                              .addOption("English (United States)", "en_US")
                              .addOption("English (South Africa)", "en_ZA")
                              .addOption("Spanish (Canada)", "es_CA")
                              .addOption("Spanish (Spain)", "es_ES")
                              .addOption("Spanish (Mexico)", "es_MX")
                              .addOption("Estonian (Estonia)", "et_EE")
                              .addOption("Persian (Iran)", "fa_IR")
                              .addOption("Finnish (Finland)", "fi_FI")
                              .addOption("French (Canada)", "fr_CA")
                              .addOption("French (Switzerland)", "fr_CH")
                              .addOption("French (France)", "fr_FR")
                              .addOption("Hebrew (Israel)", "he_IL")
                              .addOption("Hindi (India)", "hi_IN")
                              .addOption("Croatian (Croatia)", "hr_HR")
                              .addOption("Hungarian (Hungary)", "hu_HU")
                              .addOption("Armenian (Armenia)", "hy_AM")
                              .addOption("Indonesian (Indonesia)", "id_ID")
                              .addOption("Italian (Italy)", "it_IT")
                              .addOption("Japanese (Japan)", "ja_JP")
                              .addOption("Georgian (Georgia)", "ka_GE")
                              .addOption("Korean (South Korea)", "ko_KR")
                              .addOption("Lithuanian (Lithuania)", "lt_LT")
                              .addOption("Latvian (Latvia)", "lv_LV")
                              .addOption("Nepali (Nepal)", "ne_NP")
                              .addOption("Dutch (Belgium)", "nl_BE")
                              .addOption("Dutch (Netherlands)", "nl_NL")
                              .addOption("Norwegian (Norway)", "no_NO")
                              .addOption("Polish (Poland)", "pl_PL")
                              .addOption("Portuguese (Brazil)", "pt_BR")
                              .addOption("Portuguese (Portugal)", "pt_PT")
                              .addOption("Romanian (Romania)", "ro_RO")
                              .addOption("Russian (Russia)", "ru_RU")
                              .addOption("Slovenian (Slovenia)", "sl_SI")
                              .addOption("Albanian (Albania)", "sq_AL")
                              .addOption("Serbian (Serbia)", "sr_RS")
                              .addOption("Swedish (Sweden)", "sv_SE")
                              .addOption("Turkish (Turkey)", "tr_TR")
                              .addOption("Ukrainian (Ukraine)", "uk_UA")
                              .addOption("Chinese (China)", "zh_CN")
                              .addOption("Chinese (Taiwan)", "zh_TW"))

        data_types = SelectBox("Data Type") \
            .addOption("String", "String") \
            .addOption("Integer", "Integer") \
            .addOption("Float", "Float") \
            .addOption("Long", "Long") \
            .addOption("Boolean", "Boolean") \
            .addOption("Date", "Date") \
            .addOption("Timestamp", "Timestamp")

        add_name_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomNameGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomNameGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomNameGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(SelectBox("Sub Type")
                                       .addOption("Full Name", "Full Name")
                                       .addOption("First Name", "First Name")
                                       .addOption("Last Name", "Last Name")
                                       .bindProperty("record.RandomNameGenerator.subType"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomNameGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_address_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomAddressGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomAddressGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomAddressGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "0.5fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomAddressGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_email_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomEmailGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomEmailGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomEmailGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomEmailGenerator.nullCount"), "1fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_phone_number_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomPhoneNumberGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomPhoneNumberGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomPhoneNumberGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Pattern (Optional)").bindPlaceholder("(###) ###-####").bindProperty("record.RandomPhoneNumberGenerator.phoneNumPattern"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomPhoneNumberGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_random_string_uuid_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomStringIDGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomStringIDGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomStringIDGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomStringIDGenerator.nullCount"), "1fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_random_int_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomIntGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomIntGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomIntGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Start Value").bindPlaceholder("1").bindProperty("record.RandomIntGenerator.startValue"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("End Value").bindPlaceholder("1000").bindProperty("record.RandomIntGenerator.endValue"), "1fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomIntGenerator.nullCount"), "1fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_random_float_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomFloatGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomFloatGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomFloatGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Start Value").bindPlaceholder("1.5").bindProperty("record.RandomFloatGenerator.startValue"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("End Value").bindPlaceholder("99.2").bindProperty("record.RandomFloatGenerator.endValue"), "1fr")
                            .addColumn(TextBox("Decimal Places (Optional)").bindPlaceholder("2").bindProperty("record.RandomFloatGenerator.decimalPlaces"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomFloatGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_random_element_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomElementGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomElementGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomElementGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(SelectBox("List Of Values", mode = "tags").bindProperty("record.RandomElementGenerator.randomValueList"),"1fr",overflow="visible")

                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomElementGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_date_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomDateGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomDateGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomDateGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Start Date").bindPlaceholder("2024-01-15").bindProperty("record.RandomDateGenerator.startDate"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("End Date").bindPlaceholder("2024-06-15").bindProperty("record.RandomDateGenerator.endDate"), "1fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomDateGenerator.nullCount"), "1fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_datetime_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomDateTimeGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomDateTimeGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomDateTimeGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "1fr")
                            .addColumn(TextBox("Start DateTime").bindPlaceholder("2024-01-15 08:55:41").bindProperty("record.RandomDateTimeGenerator.startDateTime"), "1fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("End DateTime").bindPlaceholder("2024-06-15 11:20:31").bindProperty("record.RandomDateTimeGenerator.endDateTime"), "1fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomDateTimeGenerator.nullCount"), "1fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_boolean_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomBooleanGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomBooleanGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomBooleanGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "0.5fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomBooleanGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        add_foreign_key_generator = Condition() \
            .ifEqual(PropExpr("record.kind"), StringExpr("RandomForeignKeyGenerator")) \
            .then(
            Card(header=TitleElement(title=""), collapsible=True, collapsed=False)
                .addElement(
                ColumnsLayout(("1rem"), alignY=("end")) \
                    .addColumn(
                    StackLayout()
                        .addElement(
                        ColumnsLayout(gap="1rem")
                            .addColumn(TextBox("Column Name").bindPlaceholder("").bindProperty("record.RandomForeignKeyGenerator.columnName"), "1fr",overflow="visible")
                            .addColumn(data_types.bindProperty("record.RandomForeignKeyGenerator.dataType"),"1fr",overflow="visible")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(providers, "0.5fr")
                            .addColumn(TextBox("Reference Table Name").bindPlaceholder("catalog_name.database_name.table_name").bindProperty("record.RandomForeignKeyGenerator.refTable"), "0.5fr")
                    )
                        .addElement(
                        ColumnsLayout(("1rem"))
                            .addColumn(TextBox("Reference Column Name").bindPlaceholder("customer_id").bindProperty("record.RandomForeignKeyGenerator.refColumn"), "0.5fr")
                            .addColumn(TextBox("Null Percentage (Optional)").bindPlaceholder("20").bindProperty("record.RandomForeignKeyGenerator.nullCount"), "0.5fr")
                    ),
                    "1fr",
                    overflow=("visible")
                )
            )
        )

        dataGenTab = StackLayout(gap=("1rem"), height=("100bh")) \
            .addElement(
            OrderedList("Data Generation")
                .bindProperty("dataGenElements")
                .setEmptyContainerText("Add Column")
                .addElement(
                add_name_generator
            )
                .addElement(
                add_address_generator
            )
                .addElement(
                add_email_generator
            )
                .addElement(
                add_phone_number_generator
            )
                .addElement(
                add_random_int_generator
            )
                .addElement(
                add_random_string_uuid_generator
            )
                .addElement(
                add_boolean_generator
            )
                .addElement(
                add_random_element_generator
            )
                .addElement(
                add_date_generator
            )
                .addElement(
                add_datetime_generator
            )
                .addElement(
                add_random_float_generator
            )
                .addElement(
                add_foreign_key_generator
            )
        ) \
            .addElement(SimpleButtonLayout("Add Columns", self.onButtonClick))

        jsonSchemaPlaceholder = """
            {
                "type": "struct",
                "fields": [
                    {
                    "name": "id",
                    "type": "integer",
                    "nullable": true
                    },
                    {
                    "name": "name",
                    "type": {
                        "type": "struct",
                        "fields": [
                        {
                            "name": "first_name",
                            "type": "string",
                            "nullable": true
                        },
                        {
                            "name": "last_name",
                            "type": "string",
                            "nullable": true
                        }
                        ]
                    },
                    "nullable": true
                    },
                    {
                    "name": "email",
                    "type": "string",
                    "nullable": true
                    }
                ]
            }
        """

        settingsTab = StackLayout(height=("100bh")) \
            .addElement(Editor(height=("100%"), language='json').withSchemaSuggestions().bindProperty("jsonSchema"))

        tabs = Tabs() \
            .bindProperty("activeTab") \
            .addTabPane(
            TabPane("Random Data Generator", "random_data_gen").addElement(dataGenTab)
        ) \
            .addTabPane(
            TabPane("JSON Schema", "random_data_gen_from_json").addElement(settingsTab)
        )


        return (DatasetDialog("SyntheticDataGenerator")
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
                                .addField(TextBox("Row Count").bindPlaceholder("10"), "rowCount")
                        )
                    )
                ),
                "auto"
            )
                .addColumn(tabs, "5fr")
        ).addSection(
            "SCHEMA",
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                SchemaTable("").bindProperty("schema"), "7fr"
            )
        )
            .addSection(
            "PREVIEW",
            PreviewTable("").bindProperty("schema")))

    def targetDialog(self) -> DatasetDialog:
        return (DatasetDialog("SyntheticDataGenerator"))

    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = super(SyntheticDataGeneratorFormat, self).validate(context, component)
        props = component.properties

        for dataGen in component.properties.dataGenElements:
            if isinstance(dataGen, SyntheticDataGeneratorFormat().RandomNameGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Name Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Name Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.subType is None or dataGen.subType == "":
                    diagnostics.append(Diagnostic("properties.subType", "Random Name Generator: Sub type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomEmailGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Email Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Email Generator : Data type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomAddressGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Address Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Address Generator : Data type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomPhoneNumberGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Phone Number Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Phone Number Generator : Data type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomIntGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Integer Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Integer Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.startValue is None or dataGen.startValue == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Integer Generator : Start value can not be empty",SeverityLevelEnum.Error))

                if dataGen.endValue is None or dataGen.endValue == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Integer Generator : End value can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomFloatGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Float Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Float Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.startValue is None or dataGen.startValue == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Float Generator : Start value can not be empty",SeverityLevelEnum.Error))

                if dataGen.endValue is None or dataGen.endValue == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Float Generator : End value can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomStringIDGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random String UUID Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random String UUID Generator : Data type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomElementGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Element Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Element Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.randomValueList is None or len(dataGen.randomValueList) == 0:
                    diagnostics.append(Diagnostic("properties.dataType", "Random Element Generator : Value list can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomDateGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Date Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Date Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.startDate is None or dataGen.startDate == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Date Generator : Start date can not be empty",SeverityLevelEnum.Error))

                if dataGen.endDate is None or dataGen.endDate == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Date Generator : End date can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomDateTimeGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Date Time Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Date Time Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.startDateTime is None or dataGen.startDateTime == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Date Time Generator : Start datetime can not be empty",SeverityLevelEnum.Error))

                if dataGen.endDateTime is None or dataGen.endDateTime == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Date Time Generator : End datetime can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomBooleanGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Boolean Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Boolean Generator : Data type can not be empty",SeverityLevelEnum.Error))

            elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomForeignKeyGenerator):
                if dataGen.columnName is None or dataGen.columnName == "":
                    diagnostics.append(Diagnostic("properties.columnName", "Random Foreign Key Generator : Column name can not be empty",SeverityLevelEnum.Error))

                if dataGen.dataType is None or dataGen.dataType == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Foreign Key Generator : Data type can not be empty",SeverityLevelEnum.Error))

                if dataGen.refTable is None or dataGen.refTable == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Foreign Key Generator : Reference table name can not be empty",SeverityLevelEnum.Error))

                if dataGen.refColumn is None or dataGen.refColumn == "":
                    diagnostics.append(Diagnostic("properties.dataType", "Random Foreign Key Generator : Reference column name can not be empty",SeverityLevelEnum.Error))


        return diagnostics

    def onChange(self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        oldProps = oldState.properties
        newProps = newState.properties
        if oldProps.activeTab == "random_data_gen_from_json" and newProps.activeTab == "random_data_gen":
            newState = self.json_schema_to_ui_components(newState)
            newProps = newState.properties
        elif oldProps.activeTab == "random_data_gen" and newProps.activeTab == "random_data_gen_from_json":
            newState = self.ui_components_to_json_schema(newState)
            newProps = newState.properties

        dataGeneratorElements = newProps.dataGenElements
        jsonSchema = newProps.jsonSchema
        return newState.bindProperties(dataclasses.replace(newProps, jsonSchema=jsonSchema, dataGenElements=dataGeneratorElements))


    class SyntheticDataGeneratorFormatCode(ComponentCode):
        def __init__(self, props):
            self.props: SyntheticDataGeneratorFormat.SyntheticDataGeneratorProperties = props

        def sourceApply(self, spark: SparkSession) -> DataFrame:
            from prophecy.utils import synthetic_data_generator
            import json

            totalRows = int(self.props.rowCount)
            dataGenerator = synthetic_data_generator.FakeDataFrame(spark, totalRows)
            sparkDataTypes = {
                "String" : StringType,
                "Integer" : IntegerType,
                "Float" : FloatType,
                "Long" : LongType,
                "Boolean" : BooleanType,
                "Date" : DateType,
                "Timestamp" : TimestampType
            }

            for dataGen in self.props.dataGenElements:
                nullCount = int(totalRows * (float(dataGen.nullCount)/100)) if (dataGen.nullCount is not None and dataGen.nullCount != "") else 0
                if isinstance(dataGen, SyntheticDataGeneratorFormat().RandomNameGenerator):
                    if dataGen.subType == "Full Name":
                        dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomFullName(), data_type=sparkDataTypes[dataGen.dataType]() , nulls=nullCount)
                    elif dataGen.subType == "First Name":
                        dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomFirstName(), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)
                    elif dataGen.subType == "Last Name":
                        dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomLastName(), data_type=sparkDataTypes[dataGen.dataType](),nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomAddressGenerator):
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomAddress(), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomEmailGenerator):
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomEmail(), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomPhoneNumberGenerator):
                    pattern = dataGen.phoneNumPattern if (dataGen.phoneNumPattern is not None and dataGen.phoneNumPattern != "") else "###-###-####"
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomPhoneNumber(pattern=pattern), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomStringIDGenerator):
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomUUID(), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomIntGenerator):
                    min = int(dataGen.startValue)
                    max = int(dataGen.endValue)
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomInt(min=min, max=max), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomFloatGenerator):
                    min = float(dataGen.startValue)
                    max = float(dataGen.endValue)
                    right_digits = int(dataGen.decimalPlaces)
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomFloat(min_value=min, max_value=max, decimal_places=right_digits), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomElementGenerator):
                    listValues = dataGen.randomValueList
                    if dataGen.dataType == "Integer":
                        listValues = list(map(int, dataGen.randomValueList))
                    elif dataGen.dataType == "Float":
                        listValues = list(map(float, dataGen.randomValueList))
                    elif dataGen.dataType == "Date":
                        listValues = [datetime.strptime(date, "%Y-%m-%d") for date in dataGen.randomValueList]
                    elif dataGen.dataType == "Timestamp":
                        listValues = [datetime.strptime(dateTime, "%Y-%m-%d %H:%M:%S") for dateTime in dataGen.randomValueList]
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomListElements(elements=listValues), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomDateGenerator):
                    startDate = dataGen.startDate
                    endDate = dataGen.endDate
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomDate(start_date=startDate, end_date=endDate), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomDateTimeGenerator):
                    startDateTime = dataGen.startDateTime
                    endDateTime = dataGen.endDateTime
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomDateTime(start_datetime=startDateTime, end_datetime=endDateTime), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomBooleanGenerator):
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomBoolean(), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

                elif isinstance(dataGen, SyntheticDataGeneratorFormat().RandomForeignKeyGenerator):
                    refTable = dataGen.refTable
                    refColumn = dataGen.refColumn
                    dataGenerator = dataGenerator.addColumn(dataGen.columnName, synthetic_data_generator.RandomForeignKey(ref_table=refTable, ref_column=refColumn), data_type=sparkDataTypes[dataGen.dataType](), nulls=nullCount)

            return dataGenerator.build()

        def targetApply(self, spark: SparkSession, in0: DataFrame):
            writer = in0.writeStream
            writer.start()