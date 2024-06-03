package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_log_dw_view_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("date_time").cast(LongType).as("date_time"),
      col("additional_clearing_events"),
      col("log_impbus_auction_event"),
      col("log_dw_view")
    )

}
