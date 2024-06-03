package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time {

  def apply(context: Context, in1: DataFrame, in: DataFrame): DataFrame = {
    val df_Partition_by_Key_UnionAll_normalize_schema_1_2 =
      Partition_by_Key_UnionAll_normalize_schema_1_2(context, in)
    val df_Partition_by_Key_UnionAll_normalize_schema_0_2 =
      Partition_by_Key_UnionAll_normalize_schema_0_2(context, in1)
    val df_Partition_by_Key_UnionAll_2 = Partition_by_Key_UnionAll_2(
      context,
      df_Partition_by_Key_UnionAll_normalize_schema_1_2,
      df_Partition_by_Key_UnionAll_normalize_schema_0_2
    )
    val df_Partition_by_Key_4 =
      Partition_by_Key_4(context, df_Partition_by_Key_UnionAll_2)
    val df_Filter_by_Expression_4 =
      Filter_by_Expression_4(context, df_Partition_by_Key_4)
    val df_Checkpointed_Sort__Partial_Sort_4 =
      Checkpointed_Sort__Partial_Sort_4(context, df_Filter_by_Expression_4)
    df_Checkpointed_Sort__Partial_Sort_4
  }

}
