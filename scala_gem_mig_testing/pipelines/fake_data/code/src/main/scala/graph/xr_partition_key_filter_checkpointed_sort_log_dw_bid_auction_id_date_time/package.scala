package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time {

  def apply(context: Context, in1: DataFrame, in: DataFrame): DataFrame = {
    val df_Partition_by_Key_UnionAll_normalize_schema_1 =
      Partition_by_Key_UnionAll_normalize_schema_1(context, in)
    val df_Partition_by_Key_UnionAll_normalize_schema_0 =
      Partition_by_Key_UnionAll_normalize_schema_0(context, in1)
    val df_Partition_by_Key_UnionAll = Partition_by_Key_UnionAll(
      context,
      df_Partition_by_Key_UnionAll_normalize_schema_1,
      df_Partition_by_Key_UnionAll_normalize_schema_0
    )
    val df_Partition_by_Key =
      Partition_by_Key(context, df_Partition_by_Key_UnionAll)
    val df_Filter_by_Expression =
      Filter_by_Expression(context, df_Partition_by_Key)
    val df_Checkpointed_Sort__Partial_Sort =
      Checkpointed_Sort__Partial_Sort(context, df_Filter_by_Expression)
    df_Checkpointed_Sort__Partial_Sort
  }

}
