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

object Filter_by_Expression_3 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      is_not_null(col("auction_id_64")).and(col("auction_id_64") =!= lit(0))
    )

}
