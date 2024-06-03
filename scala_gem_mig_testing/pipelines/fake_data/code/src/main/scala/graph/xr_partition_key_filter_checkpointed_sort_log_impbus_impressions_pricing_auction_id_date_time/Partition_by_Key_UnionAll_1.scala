package graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Partition_by_Key_UnionAll_1 {

  def apply(
    context: Context,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame
  ): DataFrame = List(in4, in3, in2).flatMap(Option(_)).reduce(_.unionAll(_))

}
