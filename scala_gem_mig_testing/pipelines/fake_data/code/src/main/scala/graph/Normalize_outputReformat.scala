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

object Normalize_outputReformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("video_slot"),
      col("log_impbus_impressions_pricing_dup"),
      col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
      col("is_transactable").cast(ByteType).as("is_transactable"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("log_impbus_impressions_pricing"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("log_impbus_impressions_pricing_count")
        .cast(IntegerType)
        .as("log_impbus_impressions_pricing_count"),
      col("log_impbus_impressions"),
      col("is_deferred_impression").cast(ByteType).as("is_deferred_impression"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("is_transacted_previously")
        .cast(ByteType)
        .as("is_transacted_previously"),
      col("log_dw_view"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("imp_type"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("log_impbus_preempt"),
      col("log_impbus_preempt_dup"),
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("log_dw_bid_curator"),
      col("log_impbus_auction_event"),
      col("has_null_bid").cast(ByteType).as("has_null_bid"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("log_impbus_preempt_count")
        .cast(IntegerType)
        .as("log_impbus_preempt_count"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("log_dw_bid_deal"),
      col("date_time").cast(LongType).as("date_time"),
      col("log_impbus_view"),
      col("member_id"),
      col("additional_clearing_events"),
      col("log_dw_bid_last"),
      col("log_dw_bid"),
      col("auction_id_64").cast(LongType).as("auction_id_64")
    )

}
