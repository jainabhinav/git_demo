package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Partition_by_Key_1 = Partition_by_Key_1(context, in)
    val df_Filter_by_Expression_1 =
      Filter_by_Expression_1(context, df_Partition_by_Key_1)
    val df_Checkpointed_Sort__Partial_Sort_1 =
      Checkpointed_Sort__Partial_Sort_1(context, df_Filter_by_Expression_1)
    df_Checkpointed_Sort__Partial_Sort_1
  }

}
