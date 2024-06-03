package graph.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Partition_by_Key_UnionAll_normalize_schema_1_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("external_bidrequest_imp_id")
        .cast(LongType)
        .as("external_bidrequest_imp_id"),
      col("buyer_bid").cast(DoubleType).as("buyer_bid"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("vp_bitmap").cast(LongType).as("vp_bitmap"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("bid_price_type").cast(IntegerType).as("bid_price_type"),
      col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
      col("spend_protection_pixel_id")
        .cast(IntegerType)
        .as("spend_protection_pixel_id"),
      col("external_campaign_id").cast(StringType).as("external_campaign_id"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("buyer_exchange_rate").cast(DoubleType).as("buyer_exchange_rate"),
      col("imp_transacted").cast(IntegerType).as("imp_transacted"),
      struct(
        col("log_product_ads.product_feed_id").as("product_feed_id"),
        col("log_product_ads.item_selection_strategy_id").as(
          "item_selection_strategy_id"
        ),
        col("log_product_ads.product_uuid").as("product_uuid")
      ).cast(
          StructType(
            Array(
              StructField("product_feed_id",            IntegerType, true),
              StructField("item_selection_strategy_id", IntegerType, true),
              StructField("product_uuid",               StringType,  true)
            )
          )
        )
        .as("log_product_ads"),
      col("external_creative_id").cast(StringType).as("external_creative_id"),
      col("spend_protection").cast(IntegerType).as("spend_protection"),
      col("accept_timestamp").cast(LongType).as("accept_timestamp"),
      col("vp_expose_categories").cast(IntegerType).as("vp_expose_categories"),
      col("height").cast(IntegerType).as("height"),
      col("creative_media_subtype_id")
        .cast(IntegerType)
        .as("creative_media_subtype_id"),
      col("brand_id").cast(IntegerType).as("brand_id"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("viewdef_definition_id_buyer_member")
        .cast(IntegerType)
        .as("viewdef_definition_id_buyer_member"),
      col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
      col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
      struct(
        col("buyer_transaction_def.transaction_event").as("transaction_event"),
        col("buyer_transaction_def.transaction_event_type_id").as(
          "transaction_event_type_id"
        )
      ).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
        .as("buyer_transaction_def"),
      col("creative_audit_status")
        .cast(IntegerType)
        .as("creative_audit_status"),
      col("bidder_fees").cast(DoubleType).as("bidder_fees"),
      col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
      col("buyer_currency").cast(StringType).as("buyer_currency"),
      col("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col("trust_id").cast(StringType).as("trust_id"),
      col("ip_address").cast(StringType).as("ip_address"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("date_time").cast(LongType).as("date_time"),
      col("external_bidrequest_id").cast(LongType).as("external_bidrequest_id"),
      col("seller_deduction").cast(DoubleType).as("seller_deduction"),
      col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
      col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
      col("cleared_direct").cast(IntegerType).as("cleared_direct"),
      col("seat_id").cast(IntegerType).as("seat_id"),
      col("fold_position").cast(IntegerType).as("fold_position"),
      col("ttl").cast(IntegerType).as("ttl"),
      col("media_type").cast(IntegerType).as("media_type"),
      col("expected_events").cast(IntegerType).as("expected_events"),
      struct(
        col("seller_transaction_def.transaction_event").as("transaction_event"),
        col("seller_transaction_def.transaction_event_type_id").as(
          "transaction_event_type_id"
        )
      ).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
        .as("seller_transaction_def"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("seller_revenue").cast(DoubleType).as("seller_revenue"),
      col("width").cast(IntegerType).as("width"),
      col("deal_id").cast(IntegerType).as("deal_id"),
      col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
      col("instance_id").cast(IntegerType).as("instance_id"),
      col("deal_type").cast(IntegerType).as("deal_type"),
      col("curated_deal_id").cast(IntegerType).as("curated_deal_id")
    )

}
