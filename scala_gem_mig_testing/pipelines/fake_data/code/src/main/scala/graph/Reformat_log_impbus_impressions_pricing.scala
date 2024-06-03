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

object Reformat_log_impbus_impressions_pricing {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("date_time")
        .cast(LongType)
        .as("date_time"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("auction_id_64")
        .cast(LongType)
        .as("auction_id_64"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("buyer_charges")
        .as("buyer_charges"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("seller_charges")
        .as("seller_charges"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("buyer_spend")
        .cast(DoubleType)
        .as("buyer_spend"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("seller_revenue")
        .cast(DoubleType)
        .as("seller_revenue"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("rate_card_auction_type")
        .cast(IntegerType)
        .as("rate_card_auction_type"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("rate_card_media_type")
        .cast(IntegerType)
        .as("rate_card_media_type"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("direct_clear")
        .cast(ByteType)
        .as("direct_clear"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("auction_timestamp")
        .cast(LongType)
        .as("auction_timestamp"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("instance_id")
        .cast(IntegerType)
        .as("instance_id"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("two_phase_reduction_applied")
        .cast(ByteType)
        .as("two_phase_reduction_applied"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("trade_agreement_id")
        .cast(IntegerType)
        .as("trade_agreement_id"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("log_timestamp")
        .cast(LongType)
        .as("log_timestamp"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("trade_agreement_info")
        .as("trade_agreement_info"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("is_buy_it_now")
        .cast(ByteType)
        .as("is_buy_it_now"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("net_buyer_spend")
        .cast(DoubleType)
        .as("net_buyer_spend"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("impression_event_pricing")
        .as("impression_event_pricing"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("counterparty_ruleset_type")
        .cast(IntegerType)
        .as("counterparty_ruleset_type"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("estimated_audience_imps")
        .cast(DoubleType)
        .as("estimated_audience_imps"),
      f_convert_log_impbus_imptracker_to_log_impbus_impressions_pricing()
        .getField("audience_imps")
        .cast(DoubleType)
        .as("audience_imps")
    )

}
