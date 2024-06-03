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

object Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("postal").cast(StringType).as("postal"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("city").cast(IntegerType).as("city"),
      col("device_id").cast(IntegerType).as("device_id"),
      col("allowed_media_types")
        .cast(ArrayType(IntegerType, true))
        .as("allowed_media_types"),
      col("client_request_id").cast(StringType).as("client_request_id"),
      col("operating_system").cast(IntegerType).as("operating_system"),
      col("package_id").cast(IntegerType).as("package_id"),
      col("tag_sizes")
        .cast(
          ArrayType(StructType(
                      Array(StructField("width",  IntegerType, true),
                            StructField("height", IntegerType, true)
                      )
                    ),
                    true
          )
        )
        .as("tag_sizes"),
      col("buyer_bid").cast(DoubleType).as("buyer_bid"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("crossdevice_groups")
        .cast(
          ArrayType(StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", LongType,    true)
                      )
                    ),
                    true
          )
        )
        .as("crossdevice_groups"),
      col("vp_bitmap").cast(LongType).as("vp_bitmap"),
      col("latitude").cast(StringType).as("latitude"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("hb_source").cast(IntegerType).as("hb_source"),
      col("view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("geo_country").cast(StringType).as("geo_country"),
      col("bid_price_type").cast(IntegerType).as("bid_price_type"),
      col("shadow_price").cast(DoubleType).as("shadow_price"),
      col("inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("is_amp").cast(ByteType).as("is_amp"),
      col("external_inv_id").cast(IntegerType).as("external_inv_id"),
      col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
      col("seller_bid_currency_code")
        .cast(StringType)
        .as("seller_bid_currency_code"),
      col("spend_protection_pixel_id")
        .cast(IntegerType)
        .as("spend_protection_pixel_id"),
      struct(col("anonymized_user_info.user_id").as("user_id"))
        .cast(StructType(Array(StructField("user_id", BinaryType, true))))
        .as("anonymized_user_info"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("eap").cast(DoubleType).as("eap"),
      col("is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      col("estimated_view_rate_over_total")
        .cast(DoubleType)
        .as("estimated_view_rate_over_total"),
      col("user_locale").cast(StringType).as("user_locale"),
      col("external_uid").cast(StringType).as("external_uid"),
      col("external_campaign_id").cast(StringType).as("external_campaign_id"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("num_of_bids").cast(IntegerType).as("num_of_bids"),
      col("engagement_rates")
        .cast(
          ArrayType(
            StructType(
              Array(StructField("engagement_rate_type",    IntegerType, true),
                    StructField("rate",                    DoubleType,  true),
                    StructField("engagement_rate_type_id", IntegerType, true)
              )
            ),
            true
          )
        )
        .as("engagement_rates"),
      col("traffic_source_code").cast(StringType).as("traffic_source_code"),
      col("browser").cast(IntegerType).as("browser"),
      col("buyer_exchange_rate").cast(DoubleType).as("buyer_exchange_rate"),
      col("call_type").cast(StringType).as("call_type"),
      col("postal_code_ext_id").cast(IntegerType).as("postal_code_ext_id"),
      col("forex_allowance").cast(DoubleType).as("forex_allowance"),
      col("pred_info").cast(IntegerType).as("pred_info"),
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
      col("inv_code").cast(StringType).as("inv_code"),
      col("emp").cast(DoubleType).as("emp"),
      col("device_make_id").cast(IntegerType).as("device_make_id"),
      col("geo_region").cast(StringType).as("geo_region"),
      col("supply_type").cast(IntegerType).as("supply_type"),
      col("spend_protection").cast(IntegerType).as("spend_protection"),
      col("vp_expose_categories").cast(IntegerType).as("vp_expose_categories"),
      col("venue_id").cast(IntegerType).as("venue_id"),
      col("personal_identifiers")
        .cast(
          ArrayType(StructType(
                      Array(StructField("identity_type",  IntegerType, true),
                            StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers"),
      col("height").cast(IntegerType).as("height"),
      col("seller_bid_currency_conversion_rate")
        .cast(DoubleType)
        .as("seller_bid_currency_conversion_rate"),
      col("apply_cost_on_default")
        .cast(IntegerType)
        .as("apply_cost_on_default"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("private_auction_eligible")
        .cast(ByteType)
        .as("private_auction_eligible"),
      col("creative_media_subtype_id")
        .cast(IntegerType)
        .as("creative_media_subtype_id"),
      col("brand_id").cast(IntegerType).as("brand_id"),
      col("audit_type").cast(IntegerType).as("audit_type"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("region_id").cast(IntegerType).as("region_id"),
      col("is_whiteops_scanned").cast(ByteType).as("is_whiteops_scanned"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("gdpr_consent_cookie").cast(StringType).as("gdpr_consent_cookie"),
      col("reserve_price").cast(DoubleType).as("reserve_price"),
      col("viewdef_definition_id_buyer_member")
        .cast(IntegerType)
        .as("viewdef_definition_id_buyer_member"),
      col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("age").cast(IntegerType).as("age"),
      col("browser_code_id").cast(IntegerType).as("browser_code_id"),
      struct(col("auction_url.site_url").as("site_url"))
        .cast(StructType(Array(StructField("site_url", StringType, true))))
        .as("auction_url"),
      col("longitude").cast(StringType).as("longitude"),
      col("visibility_profile_id")
        .cast(IntegerType)
        .as("visibility_profile_id"),
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
      col("device_type").cast(IntegerType).as("device_type"),
      col("imp_bid_on").cast(IntegerType).as("imp_bid_on"),
      col("creative_audit_status")
        .cast(IntegerType)
        .as("creative_audit_status"),
      col("truncate_ip").cast(IntegerType).as("truncate_ip"),
      col("dma").cast(IntegerType).as("dma"),
      col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
      col("uid_source").cast(IntegerType).as("uid_source"),
      col("stitch_group_id").cast(StringType).as("stitch_group_id"),
      col("is_performance").cast(IntegerType).as("is_performance"),
      col("media_company_id").cast(IntegerType).as("media_company_id"),
      col("buyer_currency").cast(StringType).as("buyer_currency"),
      col("media_buy_rev_share_pct")
        .cast(DoubleType)
        .as("media_buy_rev_share_pct"),
      col("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col("ss_native_assembly_enabled")
        .cast(ByteType)
        .as("ss_native_assembly_enabled"),
      col("mobile_app_instance_id")
        .cast(IntegerType)
        .as("mobile_app_instance_id"),
      struct(col("geo_location.latitude").as("latitude"),
             col("geo_location.longitude").as("longitude")
      ).cast(
          StructType(
            Array(StructField("latitude",  DoubleType, true),
                  StructField("longitude", DoubleType, true)
            )
          )
        )
        .as("geo_location"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      col("language").cast(IntegerType).as("language"),
      col("seller_exchange_rate").cast(DoubleType).as("seller_exchange_rate"),
      col("ozone_id").cast(IntegerType).as("ozone_id"),
      col("site_domain").cast(StringType).as("site_domain"),
      col("hashed_ip").cast(StringType).as("hashed_ip"),
      col("chrome_traffic_label").cast(IntegerType).as("chrome_traffic_label"),
      col("ip_address").cast(StringType).as("ip_address"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("date_time").cast(LongType).as("date_time"),
      col("device_unique_id").cast(StringType).as("device_unique_id"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("carrier_id").cast(IntegerType).as("carrier_id"),
      col("openrtb_req_subdomain").cast(StringType).as("openrtb_req_subdomain"),
      col("imp_rejecter_do_auction")
        .cast(ByteType)
        .as("imp_rejecter_do_auction"),
      col("media_buy_cost").cast(DoubleType).as("media_buy_cost"),
      col("is_prebid").cast(ByteType).as("is_prebid"),
      col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
      col("inventory_source_id").cast(IntegerType).as("inventory_source_id"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("cookie_age").cast(IntegerType).as("cookie_age"),
      col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
      col("estimated_view_rate").cast(DoubleType).as("estimated_view_rate"),
      col("sdk_version").cast(StringType).as("sdk_version"),
      col("auction_duration_ms").cast(IntegerType).as("auction_duration_ms"),
      col("seller_currency").cast(StringType).as("seller_currency"),
      col("is_exclusive").cast(IntegerType).as("is_exclusive"),
      col("creative_duration").cast(IntegerType).as("creative_duration"),
      struct(
        col("predicted_video_view_info.iab_view_rate_over_measured").as(
          "iab_view_rate_over_measured"
        ),
        col("predicted_video_view_info.iab_view_rate_over_total").as(
          "iab_view_rate_over_total"
        ),
        col("predicted_video_view_info.predicted_100pv50pd_video_view_rate").as(
          "predicted_100pv50pd_video_view_rate"
        ),
        col(
          "predicted_video_view_info.predicted_100pv50pd_video_view_rate_over_total"
        ).as("predicted_100pv50pd_video_view_rate_over_total"),
        col("predicted_video_view_info.video_completion_rate").as(
          "video_completion_rate"
        ),
        col("predicted_video_view_info.view_prediction_source").as(
          "view_prediction_source"
        )
      ).cast(
          StructType(
            Array(
              StructField("iab_view_rate_over_measured", DoubleType, true),
              StructField("iab_view_rate_over_total",    DoubleType, true),
              StructField("predicted_100pv50pd_video_view_rate",
                          DoubleType,
                          true
              ),
              StructField("predicted_100pv50pd_video_view_rate_over_total",
                          DoubleType,
                          true
              ),
              StructField("video_completion_rate",  DoubleType,  true),
              StructField("view_prediction_source", IntegerType, true)
            )
          )
        )
        .as("predicted_video_view_info"),
      col("cleared_direct").cast(IntegerType).as("cleared_direct"),
      col("seat_id").cast(IntegerType).as("seat_id"),
      col("imp_blacklist_or_fraud")
        .cast(IntegerType)
        .as("imp_blacklist_or_fraud"),
      col("datacenter_id").cast(IntegerType).as("datacenter_id"),
      col("fold_position").cast(IntegerType).as("fold_position"),
      col("impbus_id").cast(IntegerType).as("impbus_id"),
      col("external_deal_code").cast(StringType).as("external_deal_code"),
      col("ttl").cast(IntegerType).as("ttl"),
      col("personal_identifiers_experimental")
        .cast(
          ArrayType(StructType(
                      Array(StructField("identity_type",  IntegerType, true),
                            StructField("identity_value", StringType,  true)
                      )
                    ),
                    true
          )
        )
        .as("personal_identifiers_experimental"),
      col("is_imp_rejecter_applied")
        .cast(ByteType)
        .as("is_imp_rejecter_applied"),
      col("media_type").cast(IntegerType).as("media_type"),
      col("is_private_auction").cast(ByteType).as("is_private_auction"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      col("expected_events").cast(IntegerType).as("expected_events"),
      col("inventory_session_frequency")
        .cast(IntegerType)
        .as("inventory_session_frequency"),
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
      col("content_category_id").cast(IntegerType).as("content_category_id"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("subject_to_gdpr").cast(ByteType).as("subject_to_gdpr"),
      col("seller_revenue").cast(DoubleType).as("seller_revenue"),
      col("width").cast(IntegerType).as("width"),
      col("deal_id").cast(IntegerType).as("deal_id"),
      col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
      col("bidder_instance_id").cast(IntegerType).as("bidder_instance_id"),
      col("deal_type").cast(IntegerType).as("deal_type"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      col("ecp").cast(DoubleType).as("ecp"),
      col("application_id").cast(StringType).as("application_id"),
      col("is_secure").cast(IntegerType).as("is_secure"),
      col("user_tz_offset").cast(IntegerType).as("user_tz_offset"),
      col("default_referrer_url").cast(StringType).as("default_referrer_url"),
      col("operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("is_toolbar").cast(IntegerType).as("is_toolbar"),
      col("request_uuid").cast(StringType).as("request_uuid"),
      col("gender").cast(StringType).as("gender"),
      col("external_request_id").cast(StringType).as("external_request_id")
    )

}
