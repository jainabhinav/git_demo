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

object Rollup_log_impbus_preempt {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64"))
      .agg(
        date_time(context).as("date_time"),
        lit(null).cast(IntegerType).as("is_delivered"),
        is_dw(context).as("is_dw"),
        lit(null).cast(IntegerType).as("seller_member_id"),
        buyer_member_id(context).as("buyer_member_id"),
        lit(null).cast(IntegerType).as("member_id"),
        lit(null).cast(IntegerType).as("publisher_id"),
        lit(null).cast(IntegerType).as("site_id"),
        lit(null).cast(IntegerType).as("tag_id"),
        lit(null).cast(IntegerType).as("advertiser_id"),
        lit(null).cast(IntegerType).as("campaign_group_id"),
        lit(null).cast(IntegerType).as("campaign_id"),
        lit(null).cast(IntegerType).as("insertion_order_id"),
        imp_type(context).as("imp_type"),
        first(lit(0)).as("is_transactable"),
        first(lit(0)).as("is_transacted_previously"),
        first(lit(0)).as("is_deferred_impression"),
        first(lit(0)).as("has_null_bid"),
        log_impbus_impressions(context).as("log_impbus_impressions"),
        sum(lit(1)).as("log_impbus_preempt_count"),
        last(col("w_1463972")).as("log_impbus_preempt"),
        log_impbus_preempt_dup(context).as("log_impbus_preempt_dup"),
        lit(null).cast(IntegerType).as("log_impbus_impressions_pricing_count"),
        log_impbus_impressions_pricing(context)
          .as("log_impbus_impressions_pricing"),
        log_impbus_impressions_pricing_dup(context)
          .as("log_impbus_impressions_pricing_dup")
      )

  def log_impbus_preempt_dup(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        is_not_null(col("w_1463972").getField("accept_timestamp"))
          .and(is_not_null(col("accept_timestamp").cast(LongType)))
          .and(
            col("accept_timestamp").cast(LongType) < col("w_1463972")
              .getField("accept_timestamp")
          ),
        col("w_1463972")
      ).when(col("w_1463972").isNull, col("w_1463972"))
        .otherwise(
          struct(
            col("date_time").cast(LongType).as("date_time"),
            col("auction_id_64").cast(LongType).as("auction_id_64"),
            col("imp_transacted").cast(IntegerType).as("imp_transacted"),
            col("buyer_spend"),
            col("seller_revenue"),
            col("bidder_fees"),
            col("instance_id").cast(IntegerType).as("instance_id"),
            col("fold_position").cast(IntegerType).as("fold_position"),
            col("seller_deduction"),
            col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
            col("creative_id").cast(IntegerType).as("creative_id"),
            col("cleared_direct").cast(IntegerType).as("cleared_direct"),
            col("buyer_currency"),
            col("buyer_exchange_rate"),
            col("width").cast(IntegerType).as("width"),
            col("height").cast(IntegerType).as("height"),
            col("brand_id").cast(IntegerType).as("brand_id"),
            col("creative_audit_status")
              .cast(IntegerType)
              .as("creative_audit_status"),
            col("is_creative_hosted")
              .cast(IntegerType)
              .as("is_creative_hosted"),
            col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
            col("vp_expose_categories")
              .cast(IntegerType)
              .as("vp_expose_categories"),
            col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
            col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
            col("bidder_id").cast(IntegerType).as("bidder_id"),
            col("deal_id").cast(IntegerType).as("deal_id"),
            col("imp_type").cast(IntegerType).as("imp_type"),
            col("is_dw").cast(IntegerType).as("is_dw"),
            col("vp_bitmap").cast(LongType).as("vp_bitmap"),
            col("ttl").cast(IntegerType).as("ttl"),
            col("view_detection_enabled")
              .cast(IntegerType)
              .as("view_detection_enabled"),
            col("media_type").cast(IntegerType).as("media_type"),
            col("auction_timestamp").cast(LongType).as("auction_timestamp"),
            col("spend_protection").cast(IntegerType).as("spend_protection"),
            col("viewdef_definition_id_buyer_member")
              .cast(IntegerType)
              .as("viewdef_definition_id_buyer_member"),
            col("deal_type").cast(IntegerType).as("deal_type"),
            col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
            col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
            col("bid_price_type").cast(IntegerType).as("bid_price_type"),
            col("spend_protection_pixel_id")
              .cast(IntegerType)
              .as("spend_protection_pixel_id"),
            col("ip_address"),
            col("buyer_transaction_def"),
            col("seller_transaction_def"),
            col("buyer_bid"),
            col("expected_events").cast(IntegerType).as("expected_events"),
            col("accept_timestamp").cast(LongType).as("accept_timestamp"),
            col("external_creative_id"),
            col("seat_id").cast(IntegerType).as("seat_id"),
            col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
            col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
            col("external_campaign_id"),
            col("trust_id"),
            col("log_product_ads"),
            col("external_bidrequest_id")
              .cast(LongType)
              .as("external_bidrequest_id"),
            col("external_bidrequest_imp_id")
              .cast(LongType)
              .as("external_bidrequest_imp_id"),
            col("creative_media_subtype_id")
              .cast(IntegerType)
              .as("creative_media_subtype_id")
          )
        )
    )
  }

  def date_time(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        is_not_null(col("w_1463972").getField("accept_timestamp"))
          .and(is_not_null(col("accept_timestamp").cast(LongType)))
          .and(
            col("accept_timestamp").cast(LongType) < col("w_1463972")
              .getField("accept_timestamp")
          ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("imp_transacted").cast(IntegerType).as("imp_transacted"),
          col("buyer_spend"),
          col("seller_revenue"),
          col("bidder_fees"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("fold_position").cast(IntegerType).as("fold_position"),
          col("seller_deduction"),
          col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("cleared_direct").cast(IntegerType).as("cleared_direct"),
          col("buyer_currency"),
          col("buyer_exchange_rate"),
          col("width").cast(IntegerType).as("width"),
          col("height").cast(IntegerType).as("height"),
          col("brand_id").cast(IntegerType).as("brand_id"),
          col("creative_audit_status")
            .cast(IntegerType)
            .as("creative_audit_status"),
          col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
          col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
          col("vp_expose_categories")
            .cast(IntegerType)
            .as("vp_expose_categories"),
          col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
          col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
          col("bidder_id").cast(IntegerType).as("bidder_id"),
          col("deal_id").cast(IntegerType).as("deal_id"),
          col("imp_type").cast(IntegerType).as("imp_type"),
          col("is_dw").cast(IntegerType).as("is_dw"),
          col("vp_bitmap").cast(LongType).as("vp_bitmap"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("view_detection_enabled")
            .cast(IntegerType)
            .as("view_detection_enabled"),
          col("media_type").cast(IntegerType).as("media_type"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("spend_protection").cast(IntegerType).as("spend_protection"),
          col("viewdef_definition_id_buyer_member")
            .cast(IntegerType)
            .as("viewdef_definition_id_buyer_member"),
          col("deal_type").cast(IntegerType).as("deal_type"),
          col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
          col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
          col("bid_price_type").cast(IntegerType).as("bid_price_type"),
          col("spend_protection_pixel_id")
            .cast(IntegerType)
            .as("spend_protection_pixel_id"),
          col("ip_address"),
          col("buyer_transaction_def"),
          col("seller_transaction_def"),
          col("buyer_bid"),
          col("expected_events").cast(IntegerType).as("expected_events"),
          col("accept_timestamp").cast(LongType).as("accept_timestamp"),
          col("external_creative_id"),
          col("seat_id").cast(IntegerType).as("seat_id"),
          col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
          col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
          col("external_campaign_id"),
          col("trust_id"),
          col("log_product_ads"),
          col("external_bidrequest_id")
            .cast(LongType)
            .as("external_bidrequest_id"),
          col("external_bidrequest_imp_id")
            .cast(LongType)
            .as("external_bidrequest_imp_id"),
          col("creative_media_subtype_id")
            .cast(IntegerType)
            .as("creative_media_subtype_id")
        )
      ).otherwise(
          coalesce(
            col("w_1463972"),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("imp_transacted").cast(IntegerType).as("imp_transacted"),
              col("buyer_spend"),
              col("seller_revenue"),
              col("bidder_fees"),
              col("instance_id").cast(IntegerType).as("instance_id"),
              col("fold_position").cast(IntegerType).as("fold_position"),
              col("seller_deduction"),
              col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
              col("creative_id").cast(IntegerType).as("creative_id"),
              col("cleared_direct").cast(IntegerType).as("cleared_direct"),
              col("buyer_currency"),
              col("buyer_exchange_rate"),
              col("width").cast(IntegerType).as("width"),
              col("height").cast(IntegerType).as("height"),
              col("brand_id").cast(IntegerType).as("brand_id"),
              col("creative_audit_status")
                .cast(IntegerType)
                .as("creative_audit_status"),
              col("is_creative_hosted")
                .cast(IntegerType)
                .as("is_creative_hosted"),
              col("vp_expose_domains")
                .cast(IntegerType)
                .as("vp_expose_domains"),
              col("vp_expose_categories")
                .cast(IntegerType)
                .as("vp_expose_categories"),
              col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
              col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
              col("bidder_id").cast(IntegerType).as("bidder_id"),
              col("deal_id").cast(IntegerType).as("deal_id"),
              col("imp_type").cast(IntegerType).as("imp_type"),
              col("is_dw").cast(IntegerType).as("is_dw"),
              col("vp_bitmap").cast(LongType).as("vp_bitmap"),
              col("ttl").cast(IntegerType).as("ttl"),
              col("view_detection_enabled")
                .cast(IntegerType)
                .as("view_detection_enabled"),
              col("media_type").cast(IntegerType).as("media_type"),
              col("auction_timestamp").cast(LongType).as("auction_timestamp"),
              col("spend_protection").cast(IntegerType).as("spend_protection"),
              col("viewdef_definition_id_buyer_member")
                .cast(IntegerType)
                .as("viewdef_definition_id_buyer_member"),
              col("deal_type").cast(IntegerType).as("deal_type"),
              col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
              col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
              col("bid_price_type").cast(IntegerType).as("bid_price_type"),
              col("spend_protection_pixel_id")
                .cast(IntegerType)
                .as("spend_protection_pixel_id"),
              col("ip_address"),
              col("buyer_transaction_def"),
              col("seller_transaction_def"),
              col("buyer_bid"),
              col("expected_events").cast(IntegerType).as("expected_events"),
              col("accept_timestamp").cast(LongType).as("accept_timestamp"),
              col("external_creative_id"),
              col("seat_id").cast(IntegerType).as("seat_id"),
              col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
              col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
              col("external_campaign_id"),
              col("trust_id"),
              col("log_product_ads"),
              col("external_bidrequest_id")
                .cast(LongType)
                .as("external_bidrequest_id"),
              col("external_bidrequest_imp_id")
                .cast(LongType)
                .as("external_bidrequest_imp_id"),
              col("creative_media_subtype_id")
                .cast(IntegerType)
                .as("creative_media_subtype_id")
            )
          )
        )
        .getField("date_time")
    )
  }

  def log_impbus_impressions(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                          LongType,    true),
          StructField("auction_id_64",                      LongType,    true),
          StructField("user_id_64",                         LongType,    true),
          StructField("tag_id",                             IntegerType, true),
          StructField("ip_address",                         StringType,  true),
          StructField("venue_id",                           IntegerType, true),
          StructField("site_domain",                        StringType,  true),
          StructField("width",                              IntegerType, true),
          StructField("height",                             IntegerType, true),
          StructField("geo_country",                        StringType,  true),
          StructField("geo_region",                         StringType,  true),
          StructField("gender",                             StringType,  true),
          StructField("age",                                IntegerType, true),
          StructField("bidder_id",                          IntegerType, true),
          StructField("seller_member_id",                   IntegerType, true),
          StructField("buyer_member_id",                    IntegerType, true),
          StructField("creative_id",                        IntegerType, true),
          StructField("imp_blacklist_or_fraud",             IntegerType, true),
          StructField("imp_bid_on",                         IntegerType, true),
          StructField("buyer_bid",                          DoubleType,  true),
          StructField("buyer_spend",                        DoubleType,  true),
          StructField("seller_revenue",                     DoubleType,  true),
          StructField("num_of_bids",                        IntegerType, true),
          StructField("ecp",                                DoubleType,  true),
          StructField("reserve_price",                      DoubleType,  true),
          StructField("inv_code",                           StringType,  true),
          StructField("call_type",                          StringType,  true),
          StructField("inventory_source_id",                IntegerType, true),
          StructField("cookie_age",                         IntegerType, true),
          StructField("brand_id",                           IntegerType, true),
          StructField("cleared_direct",                     IntegerType, true),
          StructField("forex_allowance",                    DoubleType,  true),
          StructField("fold_position",                      IntegerType, true),
          StructField("external_inv_id",                    IntegerType, true),
          StructField("imp_type",                           IntegerType, true),
          StructField("is_delivered",                       IntegerType, true),
          StructField("is_dw",                              IntegerType, true),
          StructField("publisher_id",                       IntegerType, true),
          StructField("site_id",                            IntegerType, true),
          StructField("content_category_id",                IntegerType, true),
          StructField("datacenter_id",                      IntegerType, true),
          StructField("eap",                                DoubleType,  true),
          StructField("user_tz_offset",                     IntegerType, true),
          StructField("user_group_id",                      IntegerType, true),
          StructField("pub_rule_id",                        IntegerType, true),
          StructField("media_type",                         IntegerType, true),
          StructField("operating_system",                   IntegerType, true),
          StructField("browser",                            IntegerType, true),
          StructField("language",                           IntegerType, true),
          StructField("application_id",                     StringType,  true),
          StructField("user_locale",                        StringType,  true),
          StructField("inventory_url_id",                   IntegerType, true),
          StructField("audit_type",                         IntegerType, true),
          StructField("shadow_price",                       DoubleType,  true),
          StructField("impbus_id",                          IntegerType, true),
          StructField("buyer_currency",                     StringType,  true),
          StructField("buyer_exchange_rate",                DoubleType,  true),
          StructField("seller_currency",                    StringType,  true),
          StructField("seller_exchange_rate",               DoubleType,  true),
          StructField("vp_expose_domains",                  IntegerType, true),
          StructField("vp_expose_categories",               IntegerType, true),
          StructField("vp_expose_pubs",                     IntegerType, true),
          StructField("vp_expose_tag",                      IntegerType, true),
          StructField("is_exclusive",                       IntegerType, true),
          StructField("bidder_instance_id",                 IntegerType, true),
          StructField("visibility_profile_id",              IntegerType, true),
          StructField("truncate_ip",                        IntegerType, true),
          StructField("device_id",                          IntegerType, true),
          StructField("carrier_id",                         IntegerType, true),
          StructField("creative_audit_status",              IntegerType, true),
          StructField("is_creative_hosted",                 IntegerType, true),
          StructField("city",                               IntegerType, true),
          StructField("latitude",                           StringType,  true),
          StructField("longitude",                          StringType,  true),
          StructField("device_unique_id",                   StringType,  true),
          StructField("supply_type",                        IntegerType, true),
          StructField("is_toolbar",                         IntegerType, true),
          StructField("deal_id",                            IntegerType, true),
          StructField("vp_bitmap",                          LongType,    true),
          StructField("ttl",                                IntegerType, true),
          StructField("view_detection_enabled",             IntegerType, true),
          StructField("ozone_id",                           IntegerType, true),
          StructField("is_performance",                     IntegerType, true),
          StructField("sdk_version",                        StringType,  true),
          StructField("inventory_session_frequency",        IntegerType, true),
          StructField("bid_price_type",                     IntegerType, true),
          StructField("device_type",                        IntegerType, true),
          StructField("dma",                                IntegerType, true),
          StructField("postal",                             StringType,  true),
          StructField("package_id",                         IntegerType, true),
          StructField("spend_protection",                   IntegerType, true),
          StructField("is_secure",                          IntegerType, true),
          StructField("estimated_view_rate",                DoubleType,  true),
          StructField("external_request_id",                StringType,  true),
          StructField("viewdef_definition_id_buyer_member", IntegerType, true),
          StructField("spend_protection_pixel_id",          IntegerType, true),
          StructField("external_uid",                       StringType,  true),
          StructField("request_uuid",                       StringType,  true),
          StructField("mobile_app_instance_id",             IntegerType, true),
          StructField("traffic_source_code",                StringType,  true),
          StructField("stitch_group_id",                    StringType,  true),
          StructField("deal_type",                          IntegerType, true),
          StructField("ym_floor_id",                        IntegerType, true),
          StructField("ym_bias_id",                         IntegerType, true),
          StructField("estimated_view_rate_over_total",     DoubleType,  true),
          StructField("device_make_id",                     IntegerType, true),
          StructField("operating_system_family_id",         IntegerType, true),
          StructField("tag_sizes",
                      ArrayType(StructType(
                                  Array(StructField("width",  IntegerType, true),
                                        StructField("height", IntegerType, true)
                                  )
                                ),
                                true
                      ),
                      true
          ),
          StructField(
            "seller_transaction_def",
            StructType(
              Array(StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
              )
            ),
            true
          ),
          StructField(
            "buyer_transaction_def",
            StructType(
              Array(StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
              )
            ),
            true
          ),
          StructField(
            "predicted_video_view_info",
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
            ),
            true
          ),
          StructField(
            "auction_url",
            StructType(Array(StructField("site_url", StringType, true))),
            true
          ),
          StructField("allowed_media_types",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("is_imp_rejecter_applied", ByteType, true),
          StructField("imp_rejecter_do_auction", ByteType, true),
          StructField("geo_location",
                      StructType(
                        Array(StructField("latitude",  DoubleType, true),
                              StructField("longitude", DoubleType, true)
                        )
                      ),
                      true
          ),
          StructField("seller_bid_currency_conversion_rate", DoubleType, true),
          StructField("seller_bid_currency_code",            StringType, true),
          StructField("is_prebid",                           ByteType,   true),
          StructField("default_referrer_url",                StringType, true),
          StructField(
            "engagement_rates",
            ArrayType(
              StructType(
                Array(StructField("engagement_rate_type",    IntegerType, true),
                      StructField("rate",                    DoubleType,  true),
                      StructField("engagement_rate_type_id", IntegerType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("fx_rate_snapshot_id",     IntegerType, true),
          StructField("payment_type",            IntegerType, true),
          StructField("apply_cost_on_default",   IntegerType, true),
          StructField("media_buy_cost",          DoubleType,  true),
          StructField("media_buy_rev_share_pct", DoubleType,  true),
          StructField("auction_duration_ms",     IntegerType, true),
          StructField("expected_events",         IntegerType, true),
          StructField(
            "anonymized_user_info",
            StructType(Array(StructField("user_id", BinaryType, true))),
            true
          ),
          StructField("region_id",                 IntegerType, true),
          StructField("media_company_id",          IntegerType, true),
          StructField("gdpr_consent_cookie",       StringType,  true),
          StructField("subject_to_gdpr",           ByteType,    true),
          StructField("browser_code_id",           IntegerType, true),
          StructField("is_prebid_server_included", IntegerType, true),
          StructField("seat_id",                   IntegerType, true),
          StructField("uid_source",                IntegerType, true),
          StructField("is_whiteops_scanned",       ByteType,    true),
          StructField("pred_info",                 IntegerType, true),
          StructField(
            "crossdevice_groups",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", LongType,    true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("is_amp",               ByteType,    true),
          StructField("hb_source",            IntegerType, true),
          StructField("external_campaign_id", StringType,  true),
          StructField(
            "log_product_ads",
            StructType(
              Array(
                StructField("product_feed_id",            IntegerType, true),
                StructField("item_selection_strategy_id", IntegerType, true),
                StructField("product_uuid",               StringType,  true)
              )
            ),
            true
          ),
          StructField("ss_native_assembly_enabled", ByteType,   true),
          StructField("emp",                        DoubleType, true),
          StructField(
            "personal_identifiers",
            ArrayType(StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "personal_identifiers_experimental",
            ArrayType(StructType(
                        Array(StructField("identity_type",  IntegerType, true),
                              StructField("identity_value", StringType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("postal_code_ext_id",        IntegerType, true),
          StructField("hashed_ip",                 StringType,  true),
          StructField("external_deal_code",        StringType,  true),
          StructField("creative_duration",         IntegerType, true),
          StructField("openrtb_req_subdomain",     StringType,  true),
          StructField("creative_media_subtype_id", IntegerType, true),
          StructField("is_private_auction",        ByteType,    true),
          StructField("private_auction_eligible",  ByteType,    true),
          StructField("client_request_id",         StringType,  true),
          StructField("chrome_traffic_label",      IntegerType, true)
        )
      )
    )
  }

  def imp_type(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        is_not_null(col("w_1463972").getField("accept_timestamp"))
          .and(is_not_null(col("accept_timestamp").cast(LongType)))
          .and(
            col("accept_timestamp").cast(LongType) < col("w_1463972")
              .getField("accept_timestamp")
          ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("imp_transacted").cast(IntegerType).as("imp_transacted"),
          col("buyer_spend"),
          col("seller_revenue"),
          col("bidder_fees"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("fold_position").cast(IntegerType).as("fold_position"),
          col("seller_deduction"),
          col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("cleared_direct").cast(IntegerType).as("cleared_direct"),
          col("buyer_currency"),
          col("buyer_exchange_rate"),
          col("width").cast(IntegerType).as("width"),
          col("height").cast(IntegerType).as("height"),
          col("brand_id").cast(IntegerType).as("brand_id"),
          col("creative_audit_status")
            .cast(IntegerType)
            .as("creative_audit_status"),
          col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
          col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
          col("vp_expose_categories")
            .cast(IntegerType)
            .as("vp_expose_categories"),
          col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
          col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
          col("bidder_id").cast(IntegerType).as("bidder_id"),
          col("deal_id").cast(IntegerType).as("deal_id"),
          col("imp_type").cast(IntegerType).as("imp_type"),
          col("is_dw").cast(IntegerType).as("is_dw"),
          col("vp_bitmap").cast(LongType).as("vp_bitmap"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("view_detection_enabled")
            .cast(IntegerType)
            .as("view_detection_enabled"),
          col("media_type").cast(IntegerType).as("media_type"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("spend_protection").cast(IntegerType).as("spend_protection"),
          col("viewdef_definition_id_buyer_member")
            .cast(IntegerType)
            .as("viewdef_definition_id_buyer_member"),
          col("deal_type").cast(IntegerType).as("deal_type"),
          col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
          col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
          col("bid_price_type").cast(IntegerType).as("bid_price_type"),
          col("spend_protection_pixel_id")
            .cast(IntegerType)
            .as("spend_protection_pixel_id"),
          col("ip_address"),
          col("buyer_transaction_def"),
          col("seller_transaction_def"),
          col("buyer_bid"),
          col("expected_events").cast(IntegerType).as("expected_events"),
          col("accept_timestamp").cast(LongType).as("accept_timestamp"),
          col("external_creative_id"),
          col("seat_id").cast(IntegerType).as("seat_id"),
          col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
          col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
          col("external_campaign_id"),
          col("trust_id"),
          col("log_product_ads"),
          col("external_bidrequest_id")
            .cast(LongType)
            .as("external_bidrequest_id"),
          col("external_bidrequest_imp_id")
            .cast(LongType)
            .as("external_bidrequest_imp_id"),
          col("creative_media_subtype_id")
            .cast(IntegerType)
            .as("creative_media_subtype_id")
        )
      ).otherwise(
          coalesce(
            col("w_1463972"),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("imp_transacted").cast(IntegerType).as("imp_transacted"),
              col("buyer_spend"),
              col("seller_revenue"),
              col("bidder_fees"),
              col("instance_id").cast(IntegerType).as("instance_id"),
              col("fold_position").cast(IntegerType).as("fold_position"),
              col("seller_deduction"),
              col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
              col("creative_id").cast(IntegerType).as("creative_id"),
              col("cleared_direct").cast(IntegerType).as("cleared_direct"),
              col("buyer_currency"),
              col("buyer_exchange_rate"),
              col("width").cast(IntegerType).as("width"),
              col("height").cast(IntegerType).as("height"),
              col("brand_id").cast(IntegerType).as("brand_id"),
              col("creative_audit_status")
                .cast(IntegerType)
                .as("creative_audit_status"),
              col("is_creative_hosted")
                .cast(IntegerType)
                .as("is_creative_hosted"),
              col("vp_expose_domains")
                .cast(IntegerType)
                .as("vp_expose_domains"),
              col("vp_expose_categories")
                .cast(IntegerType)
                .as("vp_expose_categories"),
              col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
              col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
              col("bidder_id").cast(IntegerType).as("bidder_id"),
              col("deal_id").cast(IntegerType).as("deal_id"),
              col("imp_type").cast(IntegerType).as("imp_type"),
              col("is_dw").cast(IntegerType).as("is_dw"),
              col("vp_bitmap").cast(LongType).as("vp_bitmap"),
              col("ttl").cast(IntegerType).as("ttl"),
              col("view_detection_enabled")
                .cast(IntegerType)
                .as("view_detection_enabled"),
              col("media_type").cast(IntegerType).as("media_type"),
              col("auction_timestamp").cast(LongType).as("auction_timestamp"),
              col("spend_protection").cast(IntegerType).as("spend_protection"),
              col("viewdef_definition_id_buyer_member")
                .cast(IntegerType)
                .as("viewdef_definition_id_buyer_member"),
              col("deal_type").cast(IntegerType).as("deal_type"),
              col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
              col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
              col("bid_price_type").cast(IntegerType).as("bid_price_type"),
              col("spend_protection_pixel_id")
                .cast(IntegerType)
                .as("spend_protection_pixel_id"),
              col("ip_address"),
              col("buyer_transaction_def"),
              col("seller_transaction_def"),
              col("buyer_bid"),
              col("expected_events").cast(IntegerType).as("expected_events"),
              col("accept_timestamp").cast(LongType).as("accept_timestamp"),
              col("external_creative_id"),
              col("seat_id").cast(IntegerType).as("seat_id"),
              col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
              col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
              col("external_campaign_id"),
              col("trust_id"),
              col("log_product_ads"),
              col("external_bidrequest_id")
                .cast(LongType)
                .as("external_bidrequest_id"),
              col("external_bidrequest_imp_id")
                .cast(LongType)
                .as("external_bidrequest_imp_id"),
              col("creative_media_subtype_id")
                .cast(IntegerType)
                .as("creative_media_subtype_id")
            )
          )
        )
        .getField("imp_type")
    )
  }

  def log_impbus_impressions_pricing_dup(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",     LongType, true),
          StructField("auction_id_64", LongType, true),
          StructField(
            "buyer_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        ByteType,    true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",                 IntegerType, true),
                        StructField("amount",                  DoubleType,  true),
                        StructField("rate",                    DoubleType,  true),
                        StructField("is_deduction",            ByteType,    true),
                        StructField("is_media_cost_dependent", ByteType,    true),
                        StructField("data_member_id",          IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          ByteType,    true)
              )
            ),
            true
          ),
          StructField(
            "seller_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        ByteType,    true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",                 IntegerType, true),
                        StructField("amount",                  DoubleType,  true),
                        StructField("rate",                    DoubleType,  true),
                        StructField("is_deduction",            ByteType,    true),
                        StructField("is_media_cost_dependent", ByteType,    true),
                        StructField("data_member_id",          IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          ByteType,    true)
              )
            ),
            true
          ),
          StructField("buyer_spend",                 DoubleType,  true),
          StructField("seller_revenue",              DoubleType,  true),
          StructField("rate_card_auction_type",      IntegerType, true),
          StructField("rate_card_media_type",        IntegerType, true),
          StructField("direct_clear",                ByteType,    true),
          StructField("auction_timestamp",           LongType,    true),
          StructField("instance_id",                 IntegerType, true),
          StructField("two_phase_reduction_applied", ByteType,    true),
          StructField("trade_agreement_id",          IntegerType, true),
          StructField("log_timestamp",               LongType,    true),
          StructField(
            "trade_agreement_info",
            StructType(
              Array(
                StructField("applied_term_id",   IntegerType, true),
                StructField("applied_term_type", IntegerType, true),
                StructField("targeted_term_ids",
                            ArrayType(IntegerType, true),
                            true
                )
              )
            ),
            true
          ),
          StructField("is_buy_it_now",   ByteType,   true),
          StructField("net_buyer_spend", DoubleType, true),
          StructField(
            "impression_event_pricing",
            StructType(
              Array(
                StructField("gross_payment_value_microcents", LongType, true),
                StructField("net_payment_value_microcents",   LongType, true),
                StructField("seller_revenue_microcents",      LongType, true),
                StructField(
                  "buyer_charges",
                  StructType(
                    Array(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        ByteType,    true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", ByteType,    true),
                              StructField("is_media_cost_dependent",
                                          ByteType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          ByteType,    true)
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_charges",
                  StructType(
                    Array(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        ByteType,    true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", ByteType,    true),
                              StructField("is_media_cost_dependent",
                                          ByteType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          ByteType,    true)
                    )
                  ),
                  true
                ),
                StructField("buyer_transacted",  ByteType, true),
                StructField("seller_transacted", ByteType, true)
              )
            ),
            true
          ),
          StructField("counterparty_ruleset_type", IntegerType, true),
          StructField("estimated_audience_imps",   DoubleType,  true),
          StructField("audience_imps",             DoubleType,  true)
        )
      )
    )
  }

  def log_impbus_impressions_pricing(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",     LongType, true),
          StructField("auction_id_64", LongType, true),
          StructField(
            "buyer_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        ByteType,    true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",                 IntegerType, true),
                        StructField("amount",                  DoubleType,  true),
                        StructField("rate",                    DoubleType,  true),
                        StructField("is_deduction",            ByteType,    true),
                        StructField("is_media_cost_dependent", ByteType,    true),
                        StructField("data_member_id",          IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          ByteType,    true)
              )
            ),
            true
          ),
          StructField(
            "seller_charges",
            StructType(
              Array(
                StructField("rate_card_id", IntegerType, true),
                StructField("member_id",    IntegerType, true),
                StructField("is_dw",        ByteType,    true),
                StructField(
                  "pricing_terms",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("term_id",                 IntegerType, true),
                        StructField("amount",                  DoubleType,  true),
                        StructField("rate",                    DoubleType,  true),
                        StructField("is_deduction",            ByteType,    true),
                        StructField("is_media_cost_dependent", ByteType,    true),
                        StructField("data_member_id",          IntegerType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("fx_margin_rate_id",      IntegerType, true),
                StructField("marketplace_owner_id",   IntegerType, true),
                StructField("virtual_marketplace_id", IntegerType, true),
                StructField("amino_enabled",          ByteType,    true)
              )
            ),
            true
          ),
          StructField("buyer_spend",                 DoubleType,  true),
          StructField("seller_revenue",              DoubleType,  true),
          StructField("rate_card_auction_type",      IntegerType, true),
          StructField("rate_card_media_type",        IntegerType, true),
          StructField("direct_clear",                ByteType,    true),
          StructField("auction_timestamp",           LongType,    true),
          StructField("instance_id",                 IntegerType, true),
          StructField("two_phase_reduction_applied", ByteType,    true),
          StructField("trade_agreement_id",          IntegerType, true),
          StructField("log_timestamp",               LongType,    true),
          StructField(
            "trade_agreement_info",
            StructType(
              Array(
                StructField("applied_term_id",   IntegerType, true),
                StructField("applied_term_type", IntegerType, true),
                StructField("targeted_term_ids",
                            ArrayType(IntegerType, true),
                            true
                )
              )
            ),
            true
          ),
          StructField("is_buy_it_now",   ByteType,   true),
          StructField("net_buyer_spend", DoubleType, true),
          StructField(
            "impression_event_pricing",
            StructType(
              Array(
                StructField("gross_payment_value_microcents", LongType, true),
                StructField("net_payment_value_microcents",   LongType, true),
                StructField("seller_revenue_microcents",      LongType, true),
                StructField(
                  "buyer_charges",
                  StructType(
                    Array(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        ByteType,    true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", ByteType,    true),
                              StructField("is_media_cost_dependent",
                                          ByteType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          ByteType,    true)
                    )
                  ),
                  true
                ),
                StructField(
                  "seller_charges",
                  StructType(
                    Array(
                      StructField("rate_card_id", IntegerType, true),
                      StructField("member_id",    IntegerType, true),
                      StructField("is_dw",        ByteType,    true),
                      StructField(
                        "pricing_terms",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("term_id",      IntegerType, true),
                              StructField("amount",       DoubleType,  true),
                              StructField("rate",         DoubleType,  true),
                              StructField("is_deduction", ByteType,    true),
                              StructField("is_media_cost_dependent",
                                          ByteType,
                                          true
                              ),
                              StructField("data_member_id", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("fx_margin_rate_id",      IntegerType, true),
                      StructField("marketplace_owner_id",   IntegerType, true),
                      StructField("virtual_marketplace_id", IntegerType, true),
                      StructField("amino_enabled",          ByteType,    true)
                    )
                  ),
                  true
                ),
                StructField("buyer_transacted",  ByteType, true),
                StructField("seller_transacted", ByteType, true)
              )
            ),
            true
          ),
          StructField("counterparty_ruleset_type", IntegerType, true),
          StructField("estimated_audience_imps",   DoubleType,  true),
          StructField("audience_imps",             DoubleType,  true)
        )
      )
    )
  }

  def is_dw(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        is_not_null(col("w_1463972").getField("accept_timestamp"))
          .and(is_not_null(col("accept_timestamp").cast(LongType)))
          .and(
            col("accept_timestamp").cast(LongType) < col("w_1463972")
              .getField("accept_timestamp")
          ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("imp_transacted").cast(IntegerType).as("imp_transacted"),
          col("buyer_spend"),
          col("seller_revenue"),
          col("bidder_fees"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("fold_position").cast(IntegerType).as("fold_position"),
          col("seller_deduction"),
          col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("cleared_direct").cast(IntegerType).as("cleared_direct"),
          col("buyer_currency"),
          col("buyer_exchange_rate"),
          col("width").cast(IntegerType).as("width"),
          col("height").cast(IntegerType).as("height"),
          col("brand_id").cast(IntegerType).as("brand_id"),
          col("creative_audit_status")
            .cast(IntegerType)
            .as("creative_audit_status"),
          col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
          col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
          col("vp_expose_categories")
            .cast(IntegerType)
            .as("vp_expose_categories"),
          col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
          col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
          col("bidder_id").cast(IntegerType).as("bidder_id"),
          col("deal_id").cast(IntegerType).as("deal_id"),
          col("imp_type").cast(IntegerType).as("imp_type"),
          col("is_dw").cast(IntegerType).as("is_dw"),
          col("vp_bitmap").cast(LongType).as("vp_bitmap"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("view_detection_enabled")
            .cast(IntegerType)
            .as("view_detection_enabled"),
          col("media_type").cast(IntegerType).as("media_type"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("spend_protection").cast(IntegerType).as("spend_protection"),
          col("viewdef_definition_id_buyer_member")
            .cast(IntegerType)
            .as("viewdef_definition_id_buyer_member"),
          col("deal_type").cast(IntegerType).as("deal_type"),
          col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
          col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
          col("bid_price_type").cast(IntegerType).as("bid_price_type"),
          col("spend_protection_pixel_id")
            .cast(IntegerType)
            .as("spend_protection_pixel_id"),
          col("ip_address"),
          col("buyer_transaction_def"),
          col("seller_transaction_def"),
          col("buyer_bid"),
          col("expected_events").cast(IntegerType).as("expected_events"),
          col("accept_timestamp").cast(LongType).as("accept_timestamp"),
          col("external_creative_id"),
          col("seat_id").cast(IntegerType).as("seat_id"),
          col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
          col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
          col("external_campaign_id"),
          col("trust_id"),
          col("log_product_ads"),
          col("external_bidrequest_id")
            .cast(LongType)
            .as("external_bidrequest_id"),
          col("external_bidrequest_imp_id")
            .cast(LongType)
            .as("external_bidrequest_imp_id"),
          col("creative_media_subtype_id")
            .cast(IntegerType)
            .as("creative_media_subtype_id")
        )
      ).otherwise(
          coalesce(
            col("w_1463972"),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("imp_transacted").cast(IntegerType).as("imp_transacted"),
              col("buyer_spend"),
              col("seller_revenue"),
              col("bidder_fees"),
              col("instance_id").cast(IntegerType).as("instance_id"),
              col("fold_position").cast(IntegerType).as("fold_position"),
              col("seller_deduction"),
              col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
              col("creative_id").cast(IntegerType).as("creative_id"),
              col("cleared_direct").cast(IntegerType).as("cleared_direct"),
              col("buyer_currency"),
              col("buyer_exchange_rate"),
              col("width").cast(IntegerType).as("width"),
              col("height").cast(IntegerType).as("height"),
              col("brand_id").cast(IntegerType).as("brand_id"),
              col("creative_audit_status")
                .cast(IntegerType)
                .as("creative_audit_status"),
              col("is_creative_hosted")
                .cast(IntegerType)
                .as("is_creative_hosted"),
              col("vp_expose_domains")
                .cast(IntegerType)
                .as("vp_expose_domains"),
              col("vp_expose_categories")
                .cast(IntegerType)
                .as("vp_expose_categories"),
              col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
              col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
              col("bidder_id").cast(IntegerType).as("bidder_id"),
              col("deal_id").cast(IntegerType).as("deal_id"),
              col("imp_type").cast(IntegerType).as("imp_type"),
              col("is_dw").cast(IntegerType).as("is_dw"),
              col("vp_bitmap").cast(LongType).as("vp_bitmap"),
              col("ttl").cast(IntegerType).as("ttl"),
              col("view_detection_enabled")
                .cast(IntegerType)
                .as("view_detection_enabled"),
              col("media_type").cast(IntegerType).as("media_type"),
              col("auction_timestamp").cast(LongType).as("auction_timestamp"),
              col("spend_protection").cast(IntegerType).as("spend_protection"),
              col("viewdef_definition_id_buyer_member")
                .cast(IntegerType)
                .as("viewdef_definition_id_buyer_member"),
              col("deal_type").cast(IntegerType).as("deal_type"),
              col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
              col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
              col("bid_price_type").cast(IntegerType).as("bid_price_type"),
              col("spend_protection_pixel_id")
                .cast(IntegerType)
                .as("spend_protection_pixel_id"),
              col("ip_address"),
              col("buyer_transaction_def"),
              col("seller_transaction_def"),
              col("buyer_bid"),
              col("expected_events").cast(IntegerType).as("expected_events"),
              col("accept_timestamp").cast(LongType).as("accept_timestamp"),
              col("external_creative_id"),
              col("seat_id").cast(IntegerType).as("seat_id"),
              col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
              col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
              col("external_campaign_id"),
              col("trust_id"),
              col("log_product_ads"),
              col("external_bidrequest_id")
                .cast(LongType)
                .as("external_bidrequest_id"),
              col("external_bidrequest_imp_id")
                .cast(LongType)
                .as("external_bidrequest_imp_id"),
              col("creative_media_subtype_id")
                .cast(IntegerType)
                .as("creative_media_subtype_id")
            )
          )
        )
        .getField("is_dw")
    )
  }

  def buyer_member_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        is_not_null(col("w_1463972").getField("accept_timestamp"))
          .and(is_not_null(col("accept_timestamp").cast(LongType)))
          .and(
            col("accept_timestamp").cast(LongType) < col("w_1463972")
              .getField("accept_timestamp")
          ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("imp_transacted").cast(IntegerType).as("imp_transacted"),
          col("buyer_spend"),
          col("seller_revenue"),
          col("bidder_fees"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("fold_position").cast(IntegerType).as("fold_position"),
          col("seller_deduction"),
          col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("cleared_direct").cast(IntegerType).as("cleared_direct"),
          col("buyer_currency"),
          col("buyer_exchange_rate"),
          col("width").cast(IntegerType).as("width"),
          col("height").cast(IntegerType).as("height"),
          col("brand_id").cast(IntegerType).as("brand_id"),
          col("creative_audit_status")
            .cast(IntegerType)
            .as("creative_audit_status"),
          col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
          col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
          col("vp_expose_categories")
            .cast(IntegerType)
            .as("vp_expose_categories"),
          col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
          col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
          col("bidder_id").cast(IntegerType).as("bidder_id"),
          col("deal_id").cast(IntegerType).as("deal_id"),
          col("imp_type").cast(IntegerType).as("imp_type"),
          col("is_dw").cast(IntegerType).as("is_dw"),
          col("vp_bitmap").cast(LongType).as("vp_bitmap"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("view_detection_enabled")
            .cast(IntegerType)
            .as("view_detection_enabled"),
          col("media_type").cast(IntegerType).as("media_type"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("spend_protection").cast(IntegerType).as("spend_protection"),
          col("viewdef_definition_id_buyer_member")
            .cast(IntegerType)
            .as("viewdef_definition_id_buyer_member"),
          col("deal_type").cast(IntegerType).as("deal_type"),
          col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
          col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
          col("bid_price_type").cast(IntegerType).as("bid_price_type"),
          col("spend_protection_pixel_id")
            .cast(IntegerType)
            .as("spend_protection_pixel_id"),
          col("ip_address"),
          col("buyer_transaction_def"),
          col("seller_transaction_def"),
          col("buyer_bid"),
          col("expected_events").cast(IntegerType).as("expected_events"),
          col("accept_timestamp").cast(LongType).as("accept_timestamp"),
          col("external_creative_id"),
          col("seat_id").cast(IntegerType).as("seat_id"),
          col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
          col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
          col("external_campaign_id"),
          col("trust_id"),
          col("log_product_ads"),
          col("external_bidrequest_id")
            .cast(LongType)
            .as("external_bidrequest_id"),
          col("external_bidrequest_imp_id")
            .cast(LongType)
            .as("external_bidrequest_imp_id"),
          col("creative_media_subtype_id")
            .cast(IntegerType)
            .as("creative_media_subtype_id")
        )
      ).otherwise(
          coalesce(
            col("w_1463972"),
            struct(
              col("date_time").cast(LongType).as("date_time"),
              col("auction_id_64").cast(LongType).as("auction_id_64"),
              col("imp_transacted").cast(IntegerType).as("imp_transacted"),
              col("buyer_spend"),
              col("seller_revenue"),
              col("bidder_fees"),
              col("instance_id").cast(IntegerType).as("instance_id"),
              col("fold_position").cast(IntegerType).as("fold_position"),
              col("seller_deduction"),
              col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
              col("creative_id").cast(IntegerType).as("creative_id"),
              col("cleared_direct").cast(IntegerType).as("cleared_direct"),
              col("buyer_currency"),
              col("buyer_exchange_rate"),
              col("width").cast(IntegerType).as("width"),
              col("height").cast(IntegerType).as("height"),
              col("brand_id").cast(IntegerType).as("brand_id"),
              col("creative_audit_status")
                .cast(IntegerType)
                .as("creative_audit_status"),
              col("is_creative_hosted")
                .cast(IntegerType)
                .as("is_creative_hosted"),
              col("vp_expose_domains")
                .cast(IntegerType)
                .as("vp_expose_domains"),
              col("vp_expose_categories")
                .cast(IntegerType)
                .as("vp_expose_categories"),
              col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
              col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
              col("bidder_id").cast(IntegerType).as("bidder_id"),
              col("deal_id").cast(IntegerType).as("deal_id"),
              col("imp_type").cast(IntegerType).as("imp_type"),
              col("is_dw").cast(IntegerType).as("is_dw"),
              col("vp_bitmap").cast(LongType).as("vp_bitmap"),
              col("ttl").cast(IntegerType).as("ttl"),
              col("view_detection_enabled")
                .cast(IntegerType)
                .as("view_detection_enabled"),
              col("media_type").cast(IntegerType).as("media_type"),
              col("auction_timestamp").cast(LongType).as("auction_timestamp"),
              col("spend_protection").cast(IntegerType).as("spend_protection"),
              col("viewdef_definition_id_buyer_member")
                .cast(IntegerType)
                .as("viewdef_definition_id_buyer_member"),
              col("deal_type").cast(IntegerType).as("deal_type"),
              col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
              col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
              col("bid_price_type").cast(IntegerType).as("bid_price_type"),
              col("spend_protection_pixel_id")
                .cast(IntegerType)
                .as("spend_protection_pixel_id"),
              col("ip_address"),
              col("buyer_transaction_def"),
              col("seller_transaction_def"),
              col("buyer_bid"),
              col("expected_events").cast(IntegerType).as("expected_events"),
              col("accept_timestamp").cast(LongType).as("accept_timestamp"),
              col("external_creative_id"),
              col("seat_id").cast(IntegerType).as("seat_id"),
              col("is_prebid_server").cast(ByteType).as("is_prebid_server"),
              col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
              col("external_campaign_id"),
              col("trust_id"),
              col("log_product_ads"),
              col("external_bidrequest_id")
                .cast(LongType)
                .as("external_bidrequest_id"),
              col("external_bidrequest_imp_id")
                .cast(LongType)
                .as("external_bidrequest_imp_id"),
              col("creative_media_subtype_id")
                .cast(IntegerType)
                .as("creative_media_subtype_id")
            )
          )
        )
        .getField("buyer_member_id")
    )
  }

}
