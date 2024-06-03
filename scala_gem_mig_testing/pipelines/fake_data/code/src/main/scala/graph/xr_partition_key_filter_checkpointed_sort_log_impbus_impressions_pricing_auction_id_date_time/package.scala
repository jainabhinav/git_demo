package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time {

  def apply(
    context: Context,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Partition_by_Key_UnionAll_normalize_schema_2 =
      Partition_by_Key_UnionAll_normalize_schema_2(context, in)
    val df_Partition_by_Key_UnionAll_normalize_schema_1_1 =
      Partition_by_Key_UnionAll_normalize_schema_1_1(context, in1)
    val df_Partition_by_Key_UnionAll_normalize_schema_0_1 =
      Partition_by_Key_UnionAll_normalize_schema_0_1(context, in2)
    val df_Partition_by_Key_UnionAll_1 = Partition_by_Key_UnionAll_1(
      context,
      df_Partition_by_Key_UnionAll_normalize_schema_2,
      df_Partition_by_Key_UnionAll_normalize_schema_1_1,
      df_Partition_by_Key_UnionAll_normalize_schema_0_1
    )
    val df_Partition_by_Key_3 =
      Partition_by_Key_3(context, df_Partition_by_Key_UnionAll_1)
    val df_Filter_by_Expression_3 =
      Filter_by_Expression_3(context, df_Partition_by_Key_3)
    val df_Checkpointed_Sort__Partial_Sort_3 =
      Checkpointed_Sort__Partial_Sort_3(context, df_Filter_by_Expression_3)
    df_Checkpointed_Sort__Partial_Sort_3
  }

}
