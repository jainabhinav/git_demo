package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val df_Partition_by_Key_2 = Partition_by_Key_2(context, in)
    val df_Filter_by_Expression_2 =
      Filter_by_Expression_2(context, df_Partition_by_Key_2)
    val df_Checkpointed_Sort__Partial_Sort_2 =
      Checkpointed_Sort__Partial_Sort_2(context, df_Filter_by_Expression_2)
    df_Checkpointed_Sort__Partial_Sort_2
  }

}
