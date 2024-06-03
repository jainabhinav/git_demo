package graph.OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  oLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Filter_by_Expression_Validation_rules: String =
    "true",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_dw_curator_impressions_rules: String =
    "struct()",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_invalid_impressions_quarantine_udf_rules: String =
    "",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_invalid_impressions_quarantine_rules: String =
    "struct()",
  oLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Filter_by_Expression_is_transacted_rules: String =
    "true",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_seen_denormalized_untransacted_rules: String =
    "struct()",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_seen_denormalized_transacted_udf_rules: String =
    "",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_combined_impressions_transacted_udf_rules: String =
    "",
  oLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Filter_by_Expression_has_curator_rules: String =
    "true",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_seen_denormalized_transacted_rules: String =
    "struct()",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_combined_impressions_untransacted_rules: String =
    "struct()",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_combined_impressions_untransacted_udf_rules: String =
    "",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_stage_seen_denormalized_untransacted_udf_rules: String =
    "",
  oLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Filter_by_Expression_is_dw_rules: String =
    "true",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_dw_curator_impressions_udf_rules: String =
    "",
  apply_OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter__Reformat_agg_combined_impressions_transacted_rules: String =
    "struct()"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
