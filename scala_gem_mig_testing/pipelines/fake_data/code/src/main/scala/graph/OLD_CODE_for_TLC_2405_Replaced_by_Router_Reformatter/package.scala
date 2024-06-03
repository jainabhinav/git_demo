package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object OLD_CODE_for_TLC_2405_Replaced_by_Router_Reformatter {

  def apply(context: Context): Unit = {
    val (df_Filter_by_Expression_Validation_out1,
         df_Filter_by_Expression_Validation_out0
    ) = Filter_by_Expression_Validation(context)
    val (df_Filter_by_Expression_is_transacted_out1,
         df_Filter_by_Expression_is_transacted_out0
    ) = Filter_by_Expression_is_transacted(
      context,
      df_Filter_by_Expression_Validation_out0
    )
    val df_Reformat_agg_combined_impressions_untransacted =
      Reformat_agg_combined_impressions_untransacted(
        context,
        df_Filter_by_Expression_is_transacted_out1
      )
    val df_Filter_by_Expression_has_curator = Filter_by_Expression_has_curator(
      context,
      df_Filter_by_Expression_Validation_out0
    )
    val df_Reformat_agg_dw_curator_impressions =
      Reformat_agg_dw_curator_impressions(context,
                                          df_Filter_by_Expression_has_curator
      )
    val df_Filter_by_Expression_is_dw = Filter_by_Expression_is_dw(
      context,
      df_Filter_by_Expression_Validation_out0
    )
    val df_Reformat_stage_seen_denormalized_transacted =
      Reformat_stage_seen_denormalized_transacted(
        context,
        df_Filter_by_Expression_is_transacted_out0
      )
    val df_Reformat_stage_invalid_impressions_quarantine =
      Reformat_stage_invalid_impressions_quarantine(
        context,
        df_Filter_by_Expression_Validation_out1
      )
    val df_Reformat_agg_combined_impressions_transacted =
      Reformat_agg_combined_impressions_transacted(
        context,
        df_Filter_by_Expression_is_transacted_out0
      )
    val df_Reformat_stage_seen_denormalized_untransacted =
      Reformat_stage_seen_denormalized_untransacted(
        context,
        df_Filter_by_Expression_is_transacted_out1
      )
  }

}
