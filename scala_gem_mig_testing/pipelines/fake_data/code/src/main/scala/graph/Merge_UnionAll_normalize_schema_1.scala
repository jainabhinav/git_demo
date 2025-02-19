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

object Merge_UnionAll_normalize_schema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("is_transacted_previously")
        .cast(ByteType)
        .as("is_transacted_previously"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      log_impbus_impressions_pricing(context)
        .as("log_impbus_impressions_pricing"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      log_impbus_impressions(context).as("log_impbus_impressions"),
      col("site_id").cast(IntegerType).as("site_id"),
      log_impbus_preempt(context).as("log_impbus_preempt"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("is_deferred_impression").cast(ByteType).as("is_deferred_impression"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("log_impbus_preempt_count")
        .cast(IntegerType)
        .as("log_impbus_preempt_count"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("date_time").cast(LongType).as("date_time"),
      col("log_impbus_impressions_pricing_count")
        .cast(IntegerType)
        .as("log_impbus_impressions_pricing_count"),
      col("is_transactable").cast(ByteType).as("is_transactable"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("has_null_bid").cast(ByteType).as("has_null_bid"),
      col("member_id").cast(IntegerType).as("member_id"),
      log_impbus_impressions_pricing_dup(context)
        .as("log_impbus_impressions_pricing_dup"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      log_impbus_preempt_dup(context).as("log_impbus_preempt_dup")
    )

  def log_impbus_impressions(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("log_impbus_impressions.date_time").as("date_time"),
      col("log_impbus_impressions.auction_id_64").as("auction_id_64"),
      col("log_impbus_impressions.user_id_64").as("user_id_64"),
      col("log_impbus_impressions.tag_id").as("tag_id"),
      col("log_impbus_impressions.ip_address").as("ip_address"),
      col("log_impbus_impressions.venue_id").as("venue_id"),
      col("log_impbus_impressions.site_domain").as("site_domain"),
      col("log_impbus_impressions.width").as("width"),
      col("log_impbus_impressions.height").as("height"),
      col("log_impbus_impressions.geo_country").as("geo_country"),
      col("log_impbus_impressions.geo_region").as("geo_region"),
      col("log_impbus_impressions.gender").as("gender"),
      col("log_impbus_impressions.age").as("age"),
      col("log_impbus_impressions.bidder_id").as("bidder_id"),
      col("log_impbus_impressions.seller_member_id").as("seller_member_id"),
      col("log_impbus_impressions.buyer_member_id").as("buyer_member_id"),
      col("log_impbus_impressions.creative_id").as("creative_id"),
      col("log_impbus_impressions.imp_blacklist_or_fraud").as(
        "imp_blacklist_or_fraud"
      ),
      col("log_impbus_impressions.imp_bid_on").as("imp_bid_on"),
      col("log_impbus_impressions.buyer_bid").as("buyer_bid"),
      col("log_impbus_impressions.buyer_spend").as("buyer_spend"),
      col("log_impbus_impressions.seller_revenue").as("seller_revenue"),
      col("log_impbus_impressions.num_of_bids").as("num_of_bids"),
      col("log_impbus_impressions.ecp").as("ecp"),
      col("log_impbus_impressions.reserve_price").as("reserve_price"),
      col("log_impbus_impressions.inv_code").as("inv_code"),
      col("log_impbus_impressions.call_type").as("call_type"),
      col("log_impbus_impressions.inventory_source_id").as(
        "inventory_source_id"
      ),
      col("log_impbus_impressions.cookie_age").as("cookie_age"),
      col("log_impbus_impressions.brand_id").as("brand_id"),
      col("log_impbus_impressions.cleared_direct").as("cleared_direct"),
      col("log_impbus_impressions.forex_allowance").as("forex_allowance"),
      col("log_impbus_impressions.fold_position").as("fold_position"),
      col("log_impbus_impressions.external_inv_id").as("external_inv_id"),
      col("log_impbus_impressions.imp_type").as("imp_type"),
      col("log_impbus_impressions.is_delivered").as("is_delivered"),
      col("log_impbus_impressions.is_dw").as("is_dw"),
      col("log_impbus_impressions.publisher_id").as("publisher_id"),
      col("log_impbus_impressions.site_id").as("site_id"),
      col("log_impbus_impressions.content_category_id").as(
        "content_category_id"
      ),
      col("log_impbus_impressions.datacenter_id").as("datacenter_id"),
      col("log_impbus_impressions.eap").as("eap"),
      col("log_impbus_impressions.user_tz_offset").as("user_tz_offset"),
      col("log_impbus_impressions.user_group_id").as("user_group_id"),
      col("log_impbus_impressions.pub_rule_id").as("pub_rule_id"),
      col("log_impbus_impressions.media_type").as("media_type"),
      col("log_impbus_impressions.operating_system").as("operating_system"),
      col("log_impbus_impressions.browser").as("browser"),
      col("log_impbus_impressions.language").as("language"),
      col("log_impbus_impressions.application_id").as("application_id"),
      col("log_impbus_impressions.user_locale").as("user_locale"),
      col("log_impbus_impressions.inventory_url_id").as("inventory_url_id"),
      col("log_impbus_impressions.audit_type").as("audit_type"),
      col("log_impbus_impressions.shadow_price").as("shadow_price"),
      col("log_impbus_impressions.impbus_id").as("impbus_id"),
      col("log_impbus_impressions.buyer_currency").as("buyer_currency"),
      col("log_impbus_impressions.buyer_exchange_rate").as(
        "buyer_exchange_rate"
      ),
      col("log_impbus_impressions.seller_currency").as("seller_currency"),
      col("log_impbus_impressions.seller_exchange_rate").as(
        "seller_exchange_rate"
      ),
      col("log_impbus_impressions.vp_expose_domains").as("vp_expose_domains"),
      col("log_impbus_impressions.vp_expose_categories").as(
        "vp_expose_categories"
      ),
      col("log_impbus_impressions.vp_expose_pubs").as("vp_expose_pubs"),
      col("log_impbus_impressions.vp_expose_tag").as("vp_expose_tag"),
      col("log_impbus_impressions.is_exclusive").as("is_exclusive"),
      col("log_impbus_impressions.bidder_instance_id").as("bidder_instance_id"),
      col("log_impbus_impressions.visibility_profile_id").as(
        "visibility_profile_id"
      ),
      col("log_impbus_impressions.truncate_ip").as("truncate_ip"),
      col("log_impbus_impressions.device_id").as("device_id"),
      col("log_impbus_impressions.carrier_id").as("carrier_id"),
      col("log_impbus_impressions.creative_audit_status").as(
        "creative_audit_status"
      ),
      col("log_impbus_impressions.is_creative_hosted").as("is_creative_hosted"),
      col("log_impbus_impressions.city").as("city"),
      col("log_impbus_impressions.latitude").as("latitude"),
      col("log_impbus_impressions.longitude").as("longitude"),
      col("log_impbus_impressions.device_unique_id").as("device_unique_id"),
      col("log_impbus_impressions.supply_type").as("supply_type"),
      col("log_impbus_impressions.is_toolbar").as("is_toolbar"),
      col("log_impbus_impressions.deal_id").as("deal_id"),
      col("log_impbus_impressions.vp_bitmap").as("vp_bitmap"),
      col("log_impbus_impressions.ttl").as("ttl"),
      col("log_impbus_impressions.view_detection_enabled").as(
        "view_detection_enabled"
      ),
      col("log_impbus_impressions.ozone_id").as("ozone_id"),
      col("log_impbus_impressions.is_performance").as("is_performance"),
      col("log_impbus_impressions.sdk_version").as("sdk_version"),
      col("log_impbus_impressions.inventory_session_frequency").as(
        "inventory_session_frequency"
      ),
      col("log_impbus_impressions.bid_price_type").as("bid_price_type"),
      col("log_impbus_impressions.device_type").as("device_type"),
      col("log_impbus_impressions.dma").as("dma"),
      col("log_impbus_impressions.postal").as("postal"),
      col("log_impbus_impressions.package_id").as("package_id"),
      col("log_impbus_impressions.spend_protection").as("spend_protection"),
      col("log_impbus_impressions.is_secure").as("is_secure"),
      col("log_impbus_impressions.estimated_view_rate").as(
        "estimated_view_rate"
      ),
      col("log_impbus_impressions.external_request_id").as(
        "external_request_id"
      ),
      col("log_impbus_impressions.viewdef_definition_id_buyer_member").as(
        "viewdef_definition_id_buyer_member"
      ),
      col("log_impbus_impressions.spend_protection_pixel_id").as(
        "spend_protection_pixel_id"
      ),
      col("log_impbus_impressions.external_uid").as("external_uid"),
      col("log_impbus_impressions.request_uuid").as("request_uuid"),
      col("log_impbus_impressions.mobile_app_instance_id").as(
        "mobile_app_instance_id"
      ),
      col("log_impbus_impressions.traffic_source_code").as(
        "traffic_source_code"
      ),
      col("log_impbus_impressions.stitch_group_id").as("stitch_group_id"),
      col("log_impbus_impressions.deal_type").as("deal_type"),
      col("log_impbus_impressions.ym_floor_id").as("ym_floor_id"),
      col("log_impbus_impressions.ym_bias_id").as("ym_bias_id"),
      col("log_impbus_impressions.estimated_view_rate_over_total").as(
        "estimated_view_rate_over_total"
      ),
      col("log_impbus_impressions.device_make_id").as("device_make_id"),
      col("log_impbus_impressions.operating_system_family_id").as(
        "operating_system_family_id"
      ),
      col("log_impbus_impressions.tag_sizes").as("tag_sizes"),
      struct(
        col("log_impbus_impressions.seller_transaction_def.transaction_event")
          .as("transaction_event"),
        col(
          "log_impbus_impressions.seller_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("seller_transaction_def"),
      struct(
        col("log_impbus_impressions.buyer_transaction_def.transaction_event")
          .as("transaction_event"),
        col(
          "log_impbus_impressions.buyer_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("buyer_transaction_def"),
      struct(
        col(
          "log_impbus_impressions.predicted_video_view_info.iab_view_rate_over_measured"
        ).as("iab_view_rate_over_measured"),
        col(
          "log_impbus_impressions.predicted_video_view_info.iab_view_rate_over_total"
        ).as("iab_view_rate_over_total"),
        col(
          "log_impbus_impressions.predicted_video_view_info.predicted_100pv50pd_video_view_rate"
        ).as("predicted_100pv50pd_video_view_rate"),
        col(
          "log_impbus_impressions.predicted_video_view_info.predicted_100pv50pd_video_view_rate_over_total"
        ).as("predicted_100pv50pd_video_view_rate_over_total"),
        col(
          "log_impbus_impressions.predicted_video_view_info.video_completion_rate"
        ).as("video_completion_rate"),
        col(
          "log_impbus_impressions.predicted_video_view_info.view_prediction_source"
        ).as("view_prediction_source")
      ).as("predicted_video_view_info"),
      struct(col("log_impbus_impressions.auction_url.site_url").as("site_url"))
        .as("auction_url"),
      col("log_impbus_impressions.allowed_media_types").as(
        "allowed_media_types"
      ),
      col("log_impbus_impressions.is_imp_rejecter_applied").as(
        "is_imp_rejecter_applied"
      ),
      col("log_impbus_impressions.imp_rejecter_do_auction").as(
        "imp_rejecter_do_auction"
      ),
      struct(
        col("log_impbus_impressions.geo_location.latitude").as("latitude"),
        col("log_impbus_impressions.geo_location.longitude").as("longitude")
      ).as("geo_location"),
      col("log_impbus_impressions.seller_bid_currency_conversion_rate").as(
        "seller_bid_currency_conversion_rate"
      ),
      col("log_impbus_impressions.seller_bid_currency_code").as(
        "seller_bid_currency_code"
      ),
      col("log_impbus_impressions.is_prebid").as("is_prebid"),
      col("log_impbus_impressions.default_referrer_url").as(
        "default_referrer_url"
      ),
      col("log_impbus_impressions.engagement_rates").as("engagement_rates"),
      col("log_impbus_impressions.fx_rate_snapshot_id").as(
        "fx_rate_snapshot_id"
      ),
      col("log_impbus_impressions.payment_type").as("payment_type"),
      col("log_impbus_impressions.apply_cost_on_default").as(
        "apply_cost_on_default"
      ),
      col("log_impbus_impressions.media_buy_cost").as("media_buy_cost"),
      col("log_impbus_impressions.media_buy_rev_share_pct").as(
        "media_buy_rev_share_pct"
      ),
      col("log_impbus_impressions.auction_duration_ms").as(
        "auction_duration_ms"
      ),
      col("log_impbus_impressions.expected_events").as("expected_events"),
      struct(
        col("log_impbus_impressions.anonymized_user_info.user_id").as("user_id")
      ).as("anonymized_user_info"),
      col("log_impbus_impressions.region_id").as("region_id"),
      col("log_impbus_impressions.media_company_id").as("media_company_id"),
      col("log_impbus_impressions.gdpr_consent_cookie").as(
        "gdpr_consent_cookie"
      ),
      col("log_impbus_impressions.subject_to_gdpr").as("subject_to_gdpr"),
      col("log_impbus_impressions.browser_code_id").as("browser_code_id"),
      col("log_impbus_impressions.is_prebid_server_included").as(
        "is_prebid_server_included"
      ),
      col("log_impbus_impressions.seat_id").as("seat_id"),
      col("log_impbus_impressions.uid_source").as("uid_source"),
      col("log_impbus_impressions.is_whiteops_scanned").as(
        "is_whiteops_scanned"
      ),
      col("log_impbus_impressions.pred_info").as("pred_info"),
      col("log_impbus_impressions.crossdevice_groups").as("crossdevice_groups"),
      col("log_impbus_impressions.is_amp").as("is_amp"),
      col("log_impbus_impressions.hb_source").as("hb_source"),
      col("log_impbus_impressions.external_campaign_id").as(
        "external_campaign_id"
      ),
      struct(
        col("log_impbus_impressions.log_product_ads.product_feed_id").as(
          "product_feed_id"
        ),
        col("log_impbus_impressions.log_product_ads.item_selection_strategy_id")
          .as("item_selection_strategy_id"),
        col("log_impbus_impressions.log_product_ads.product_uuid").as(
          "product_uuid"
        )
      ).as("log_product_ads"),
      col("log_impbus_impressions.ss_native_assembly_enabled").as(
        "ss_native_assembly_enabled"
      ),
      col("log_impbus_impressions.emp").as("emp"),
      col("log_impbus_impressions.personal_identifiers").as(
        "personal_identifiers"
      ),
      col("log_impbus_impressions.personal_identifiers_experimental").as(
        "personal_identifiers_experimental"
      ),
      col("log_impbus_impressions.postal_code_ext_id").as("postal_code_ext_id"),
      col("log_impbus_impressions.hashed_ip").as("hashed_ip"),
      col("log_impbus_impressions.external_deal_code").as("external_deal_code"),
      col("log_impbus_impressions.creative_duration").as("creative_duration"),
      col("log_impbus_impressions.openrtb_req_subdomain").as(
        "openrtb_req_subdomain"
      ),
      col("log_impbus_impressions.creative_media_subtype_id").as(
        "creative_media_subtype_id"
      ),
      col("log_impbus_impressions.is_private_auction").as("is_private_auction"),
      col("log_impbus_impressions.private_auction_eligible").as(
        "private_auction_eligible"
      ),
      col("log_impbus_impressions.client_request_id").as("client_request_id"),
      col("log_impbus_impressions.chrome_traffic_label").as(
        "chrome_traffic_label"
      )
    ).cast(
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

  def log_impbus_preempt_dup(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("log_impbus_preempt_dup.date_time").as("date_time"),
      col("log_impbus_preempt_dup.auction_id_64").as("auction_id_64"),
      col("log_impbus_preempt_dup.imp_transacted").as("imp_transacted"),
      col("log_impbus_preempt_dup.buyer_spend").as("buyer_spend"),
      col("log_impbus_preempt_dup.seller_revenue").as("seller_revenue"),
      col("log_impbus_preempt_dup.bidder_fees").as("bidder_fees"),
      col("log_impbus_preempt_dup.instance_id").as("instance_id"),
      col("log_impbus_preempt_dup.fold_position").as("fold_position"),
      col("log_impbus_preempt_dup.seller_deduction").as("seller_deduction"),
      col("log_impbus_preempt_dup.buyer_member_id").as("buyer_member_id"),
      col("log_impbus_preempt_dup.creative_id").as("creative_id"),
      col("log_impbus_preempt_dup.cleared_direct").as("cleared_direct"),
      col("log_impbus_preempt_dup.buyer_currency").as("buyer_currency"),
      col("log_impbus_preempt_dup.buyer_exchange_rate").as(
        "buyer_exchange_rate"
      ),
      col("log_impbus_preempt_dup.width").as("width"),
      col("log_impbus_preempt_dup.height").as("height"),
      col("log_impbus_preempt_dup.brand_id").as("brand_id"),
      col("log_impbus_preempt_dup.creative_audit_status").as(
        "creative_audit_status"
      ),
      col("log_impbus_preempt_dup.is_creative_hosted").as("is_creative_hosted"),
      col("log_impbus_preempt_dup.vp_expose_domains").as("vp_expose_domains"),
      col("log_impbus_preempt_dup.vp_expose_categories").as(
        "vp_expose_categories"
      ),
      col("log_impbus_preempt_dup.vp_expose_pubs").as("vp_expose_pubs"),
      col("log_impbus_preempt_dup.vp_expose_tag").as("vp_expose_tag"),
      col("log_impbus_preempt_dup.bidder_id").as("bidder_id"),
      col("log_impbus_preempt_dup.deal_id").as("deal_id"),
      col("log_impbus_preempt_dup.imp_type").as("imp_type"),
      col("log_impbus_preempt_dup.is_dw").as("is_dw"),
      col("log_impbus_preempt_dup.vp_bitmap").as("vp_bitmap"),
      col("log_impbus_preempt_dup.ttl").as("ttl"),
      col("log_impbus_preempt_dup.view_detection_enabled").as(
        "view_detection_enabled"
      ),
      col("log_impbus_preempt_dup.media_type").as("media_type"),
      col("log_impbus_preempt_dup.auction_timestamp").as("auction_timestamp"),
      col("log_impbus_preempt_dup.spend_protection").as("spend_protection"),
      col("log_impbus_preempt_dup.viewdef_definition_id_buyer_member").as(
        "viewdef_definition_id_buyer_member"
      ),
      col("log_impbus_preempt_dup.deal_type").as("deal_type"),
      col("log_impbus_preempt_dup.ym_floor_id").as("ym_floor_id"),
      col("log_impbus_preempt_dup.ym_bias_id").as("ym_bias_id"),
      col("log_impbus_preempt_dup.bid_price_type").as("bid_price_type"),
      col("log_impbus_preempt_dup.spend_protection_pixel_id").as(
        "spend_protection_pixel_id"
      ),
      col("log_impbus_preempt_dup.ip_address").as("ip_address"),
      struct(
        col("log_impbus_preempt_dup.buyer_transaction_def.transaction_event")
          .as("transaction_event"),
        col(
          "log_impbus_preempt_dup.buyer_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("buyer_transaction_def"),
      struct(
        col("log_impbus_preempt_dup.seller_transaction_def.transaction_event")
          .as("transaction_event"),
        col(
          "log_impbus_preempt_dup.seller_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("seller_transaction_def"),
      col("log_impbus_preempt_dup.buyer_bid").as("buyer_bid"),
      col("log_impbus_preempt_dup.expected_events").as("expected_events"),
      col("log_impbus_preempt_dup.accept_timestamp").as("accept_timestamp"),
      col("log_impbus_preempt_dup.external_creative_id").as(
        "external_creative_id"
      ),
      col("log_impbus_preempt_dup.seat_id").as("seat_id"),
      col("log_impbus_preempt_dup.is_prebid_server").as("is_prebid_server"),
      col("log_impbus_preempt_dup.curated_deal_id").as("curated_deal_id"),
      col("log_impbus_preempt_dup.external_campaign_id").as(
        "external_campaign_id"
      ),
      col("log_impbus_preempt_dup.trust_id").as("trust_id"),
      struct(
        col("log_impbus_preempt_dup.log_product_ads.product_feed_id").as(
          "product_feed_id"
        ),
        col("log_impbus_preempt_dup.log_product_ads.item_selection_strategy_id")
          .as("item_selection_strategy_id"),
        col("log_impbus_preempt_dup.log_product_ads.product_uuid").as(
          "product_uuid"
        )
      ).as("log_product_ads"),
      col("log_impbus_preempt_dup.external_bidrequest_id").as(
        "external_bidrequest_id"
      ),
      col("log_impbus_preempt_dup.external_bidrequest_imp_id").as(
        "external_bidrequest_imp_id"
      ),
      col("log_impbus_preempt_dup.creative_media_subtype_id").as(
        "creative_media_subtype_id"
      )
    ).cast(
      StructType(
        Array(
          StructField("date_time",                          LongType,    true),
          StructField("auction_id_64",                      LongType,    true),
          StructField("imp_transacted",                     IntegerType, true),
          StructField("buyer_spend",                        DoubleType,  true),
          StructField("seller_revenue",                     DoubleType,  true),
          StructField("bidder_fees",                        DoubleType,  true),
          StructField("instance_id",                        IntegerType, true),
          StructField("fold_position",                      IntegerType, true),
          StructField("seller_deduction",                   DoubleType,  true),
          StructField("buyer_member_id",                    IntegerType, true),
          StructField("creative_id",                        IntegerType, true),
          StructField("cleared_direct",                     IntegerType, true),
          StructField("buyer_currency",                     StringType,  true),
          StructField("buyer_exchange_rate",                DoubleType,  true),
          StructField("width",                              IntegerType, true),
          StructField("height",                             IntegerType, true),
          StructField("brand_id",                           IntegerType, true),
          StructField("creative_audit_status",              IntegerType, true),
          StructField("is_creative_hosted",                 IntegerType, true),
          StructField("vp_expose_domains",                  IntegerType, true),
          StructField("vp_expose_categories",               IntegerType, true),
          StructField("vp_expose_pubs",                     IntegerType, true),
          StructField("vp_expose_tag",                      IntegerType, true),
          StructField("bidder_id",                          IntegerType, true),
          StructField("deal_id",                            IntegerType, true),
          StructField("imp_type",                           IntegerType, true),
          StructField("is_dw",                              IntegerType, true),
          StructField("vp_bitmap",                          LongType,    true),
          StructField("ttl",                                IntegerType, true),
          StructField("view_detection_enabled",             IntegerType, true),
          StructField("media_type",                         IntegerType, true),
          StructField("auction_timestamp",                  LongType,    true),
          StructField("spend_protection",                   IntegerType, true),
          StructField("viewdef_definition_id_buyer_member", IntegerType, true),
          StructField("deal_type",                          IntegerType, true),
          StructField("ym_floor_id",                        IntegerType, true),
          StructField("ym_bias_id",                         IntegerType, true),
          StructField("bid_price_type",                     IntegerType, true),
          StructField("spend_protection_pixel_id",          IntegerType, true),
          StructField("ip_address",                         StringType,  true),
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
            "seller_transaction_def",
            StructType(
              Array(StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
              )
            ),
            true
          ),
          StructField("buyer_bid",            DoubleType,  true),
          StructField("expected_events",      IntegerType, true),
          StructField("accept_timestamp",     LongType,    true),
          StructField("external_creative_id", StringType,  true),
          StructField("seat_id",              IntegerType, true),
          StructField("is_prebid_server",     ByteType,    true),
          StructField("curated_deal_id",      IntegerType, true),
          StructField("external_campaign_id", StringType,  true),
          StructField("trust_id",             StringType,  true),
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
          StructField("external_bidrequest_id",     LongType,    true),
          StructField("external_bidrequest_imp_id", LongType,    true),
          StructField("creative_media_subtype_id",  IntegerType, true)
        )
      )
    )
  }

  def log_impbus_preempt(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("log_impbus_preempt.date_time").as("date_time"),
      col("log_impbus_preempt.auction_id_64").as("auction_id_64"),
      col("log_impbus_preempt.imp_transacted").as("imp_transacted"),
      col("log_impbus_preempt.buyer_spend").as("buyer_spend"),
      col("log_impbus_preempt.seller_revenue").as("seller_revenue"),
      col("log_impbus_preempt.bidder_fees").as("bidder_fees"),
      col("log_impbus_preempt.instance_id").as("instance_id"),
      col("log_impbus_preempt.fold_position").as("fold_position"),
      col("log_impbus_preempt.seller_deduction").as("seller_deduction"),
      col("log_impbus_preempt.buyer_member_id").as("buyer_member_id"),
      col("log_impbus_preempt.creative_id").as("creative_id"),
      col("log_impbus_preempt.cleared_direct").as("cleared_direct"),
      col("log_impbus_preempt.buyer_currency").as("buyer_currency"),
      col("log_impbus_preempt.buyer_exchange_rate").as("buyer_exchange_rate"),
      col("log_impbus_preempt.width").as("width"),
      col("log_impbus_preempt.height").as("height"),
      col("log_impbus_preempt.brand_id").as("brand_id"),
      col("log_impbus_preempt.creative_audit_status").as(
        "creative_audit_status"
      ),
      col("log_impbus_preempt.is_creative_hosted").as("is_creative_hosted"),
      col("log_impbus_preempt.vp_expose_domains").as("vp_expose_domains"),
      col("log_impbus_preempt.vp_expose_categories").as("vp_expose_categories"),
      col("log_impbus_preempt.vp_expose_pubs").as("vp_expose_pubs"),
      col("log_impbus_preempt.vp_expose_tag").as("vp_expose_tag"),
      col("log_impbus_preempt.bidder_id").as("bidder_id"),
      col("log_impbus_preempt.deal_id").as("deal_id"),
      col("log_impbus_preempt.imp_type").as("imp_type"),
      col("log_impbus_preempt.is_dw").as("is_dw"),
      col("log_impbus_preempt.vp_bitmap").as("vp_bitmap"),
      col("log_impbus_preempt.ttl").as("ttl"),
      col("log_impbus_preempt.view_detection_enabled").as(
        "view_detection_enabled"
      ),
      col("log_impbus_preempt.media_type").as("media_type"),
      col("log_impbus_preempt.auction_timestamp").as("auction_timestamp"),
      col("log_impbus_preempt.spend_protection").as("spend_protection"),
      col("log_impbus_preempt.viewdef_definition_id_buyer_member").as(
        "viewdef_definition_id_buyer_member"
      ),
      col("log_impbus_preempt.deal_type").as("deal_type"),
      col("log_impbus_preempt.ym_floor_id").as("ym_floor_id"),
      col("log_impbus_preempt.ym_bias_id").as("ym_bias_id"),
      col("log_impbus_preempt.bid_price_type").as("bid_price_type"),
      col("log_impbus_preempt.spend_protection_pixel_id").as(
        "spend_protection_pixel_id"
      ),
      col("log_impbus_preempt.ip_address").as("ip_address"),
      struct(
        col("log_impbus_preempt.buyer_transaction_def.transaction_event").as(
          "transaction_event"
        ),
        col(
          "log_impbus_preempt.buyer_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("buyer_transaction_def"),
      struct(
        col("log_impbus_preempt.seller_transaction_def.transaction_event").as(
          "transaction_event"
        ),
        col(
          "log_impbus_preempt.seller_transaction_def.transaction_event_type_id"
        ).as("transaction_event_type_id")
      ).as("seller_transaction_def"),
      col("log_impbus_preempt.buyer_bid").as("buyer_bid"),
      col("log_impbus_preempt.expected_events").as("expected_events"),
      col("log_impbus_preempt.accept_timestamp").as("accept_timestamp"),
      col("log_impbus_preempt.external_creative_id").as("external_creative_id"),
      col("log_impbus_preempt.seat_id").as("seat_id"),
      col("log_impbus_preempt.is_prebid_server").as("is_prebid_server"),
      col("log_impbus_preempt.curated_deal_id").as("curated_deal_id"),
      col("log_impbus_preempt.external_campaign_id").as("external_campaign_id"),
      col("log_impbus_preempt.trust_id").as("trust_id"),
      struct(
        col("log_impbus_preempt.log_product_ads.product_feed_id").as(
          "product_feed_id"
        ),
        col("log_impbus_preempt.log_product_ads.item_selection_strategy_id").as(
          "item_selection_strategy_id"
        ),
        col("log_impbus_preempt.log_product_ads.product_uuid").as(
          "product_uuid"
        )
      ).as("log_product_ads"),
      col("log_impbus_preempt.external_bidrequest_id").as(
        "external_bidrequest_id"
      ),
      col("log_impbus_preempt.external_bidrequest_imp_id").as(
        "external_bidrequest_imp_id"
      ),
      col("log_impbus_preempt.creative_media_subtype_id").as(
        "creative_media_subtype_id"
      )
    ).cast(
      StructType(
        Array(
          StructField("date_time",                          LongType,    true),
          StructField("auction_id_64",                      LongType,    true),
          StructField("imp_transacted",                     IntegerType, true),
          StructField("buyer_spend",                        DoubleType,  true),
          StructField("seller_revenue",                     DoubleType,  true),
          StructField("bidder_fees",                        DoubleType,  true),
          StructField("instance_id",                        IntegerType, true),
          StructField("fold_position",                      IntegerType, true),
          StructField("seller_deduction",                   DoubleType,  true),
          StructField("buyer_member_id",                    IntegerType, true),
          StructField("creative_id",                        IntegerType, true),
          StructField("cleared_direct",                     IntegerType, true),
          StructField("buyer_currency",                     StringType,  true),
          StructField("buyer_exchange_rate",                DoubleType,  true),
          StructField("width",                              IntegerType, true),
          StructField("height",                             IntegerType, true),
          StructField("brand_id",                           IntegerType, true),
          StructField("creative_audit_status",              IntegerType, true),
          StructField("is_creative_hosted",                 IntegerType, true),
          StructField("vp_expose_domains",                  IntegerType, true),
          StructField("vp_expose_categories",               IntegerType, true),
          StructField("vp_expose_pubs",                     IntegerType, true),
          StructField("vp_expose_tag",                      IntegerType, true),
          StructField("bidder_id",                          IntegerType, true),
          StructField("deal_id",                            IntegerType, true),
          StructField("imp_type",                           IntegerType, true),
          StructField("is_dw",                              IntegerType, true),
          StructField("vp_bitmap",                          LongType,    true),
          StructField("ttl",                                IntegerType, true),
          StructField("view_detection_enabled",             IntegerType, true),
          StructField("media_type",                         IntegerType, true),
          StructField("auction_timestamp",                  LongType,    true),
          StructField("spend_protection",                   IntegerType, true),
          StructField("viewdef_definition_id_buyer_member", IntegerType, true),
          StructField("deal_type",                          IntegerType, true),
          StructField("ym_floor_id",                        IntegerType, true),
          StructField("ym_bias_id",                         IntegerType, true),
          StructField("bid_price_type",                     IntegerType, true),
          StructField("spend_protection_pixel_id",          IntegerType, true),
          StructField("ip_address",                         StringType,  true),
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
            "seller_transaction_def",
            StructType(
              Array(StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
              )
            ),
            true
          ),
          StructField("buyer_bid",            DoubleType,  true),
          StructField("expected_events",      IntegerType, true),
          StructField("accept_timestamp",     LongType,    true),
          StructField("external_creative_id", StringType,  true),
          StructField("seat_id",              IntegerType, true),
          StructField("is_prebid_server",     ByteType,    true),
          StructField("curated_deal_id",      IntegerType, true),
          StructField("external_campaign_id", StringType,  true),
          StructField("trust_id",             StringType,  true),
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
          StructField("external_bidrequest_id",     LongType,    true),
          StructField("external_bidrequest_imp_id", LongType,    true),
          StructField("creative_media_subtype_id",  IntegerType, true)
        )
      )
    )
  }

  def log_impbus_impressions_pricing_dup(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("log_impbus_impressions_pricing_dup.date_time").as("date_time"),
      col("log_impbus_impressions_pricing_dup.auction_id_64").as(
        "auction_id_64"
      ),
      struct(
        col("log_impbus_impressions_pricing_dup.buyer_charges.rate_card_id").as(
          "rate_card_id"
        ),
        col("log_impbus_impressions_pricing_dup.buyer_charges.member_id").as(
          "member_id"
        ),
        col("log_impbus_impressions_pricing_dup.buyer_charges.is_dw").as(
          "is_dw"
        ),
        col("log_impbus_impressions_pricing_dup.buyer_charges.pricing_terms")
          .as("pricing_terms"),
        col(
          "log_impbus_impressions_pricing_dup.buyer_charges.fx_margin_rate_id"
        ).as("fx_margin_rate_id"),
        col(
          "log_impbus_impressions_pricing_dup.buyer_charges.marketplace_owner_id"
        ).as("marketplace_owner_id"),
        col(
          "log_impbus_impressions_pricing_dup.buyer_charges.virtual_marketplace_id"
        ).as("virtual_marketplace_id"),
        col("log_impbus_impressions_pricing_dup.buyer_charges.amino_enabled")
          .as("amino_enabled")
      ).as("buyer_charges"),
      struct(
        col("log_impbus_impressions_pricing_dup.seller_charges.rate_card_id")
          .as("rate_card_id"),
        col("log_impbus_impressions_pricing_dup.seller_charges.member_id").as(
          "member_id"
        ),
        col("log_impbus_impressions_pricing_dup.seller_charges.is_dw").as(
          "is_dw"
        ),
        col("log_impbus_impressions_pricing_dup.seller_charges.pricing_terms")
          .as("pricing_terms"),
        col(
          "log_impbus_impressions_pricing_dup.seller_charges.fx_margin_rate_id"
        ).as("fx_margin_rate_id"),
        col(
          "log_impbus_impressions_pricing_dup.seller_charges.marketplace_owner_id"
        ).as("marketplace_owner_id"),
        col(
          "log_impbus_impressions_pricing_dup.seller_charges.virtual_marketplace_id"
        ).as("virtual_marketplace_id"),
        col("log_impbus_impressions_pricing_dup.seller_charges.amino_enabled")
          .as("amino_enabled")
      ).as("seller_charges"),
      col("log_impbus_impressions_pricing_dup.buyer_spend").as("buyer_spend"),
      col("log_impbus_impressions_pricing_dup.seller_revenue").as(
        "seller_revenue"
      ),
      col("log_impbus_impressions_pricing_dup.rate_card_auction_type").as(
        "rate_card_auction_type"
      ),
      col("log_impbus_impressions_pricing_dup.rate_card_media_type").as(
        "rate_card_media_type"
      ),
      col("log_impbus_impressions_pricing_dup.direct_clear").as("direct_clear"),
      col("log_impbus_impressions_pricing_dup.auction_timestamp").as(
        "auction_timestamp"
      ),
      col("log_impbus_impressions_pricing_dup.instance_id").as("instance_id"),
      col("log_impbus_impressions_pricing_dup.two_phase_reduction_applied").as(
        "two_phase_reduction_applied"
      ),
      col("log_impbus_impressions_pricing_dup.trade_agreement_id").as(
        "trade_agreement_id"
      ),
      col("log_impbus_impressions_pricing_dup.log_timestamp").as(
        "log_timestamp"
      ),
      struct(
        col(
          "log_impbus_impressions_pricing_dup.trade_agreement_info.applied_term_id"
        ).as("applied_term_id"),
        col(
          "log_impbus_impressions_pricing_dup.trade_agreement_info.applied_term_type"
        ).as("applied_term_type"),
        col(
          "log_impbus_impressions_pricing_dup.trade_agreement_info.targeted_term_ids"
        ).as("targeted_term_ids")
      ).as("trade_agreement_info"),
      col("log_impbus_impressions_pricing_dup.is_buy_it_now").as(
        "is_buy_it_now"
      ),
      col("log_impbus_impressions_pricing_dup.net_buyer_spend").as(
        "net_buyer_spend"
      ),
      struct(
        col(
          "log_impbus_impressions_pricing_dup.impression_event_pricing.gross_payment_value_microcents"
        ).as("gross_payment_value_microcents"),
        col(
          "log_impbus_impressions_pricing_dup.impression_event_pricing.net_payment_value_microcents"
        ).as("net_payment_value_microcents"),
        col(
          "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_revenue_microcents"
        ).as("seller_revenue_microcents"),
        struct(
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.rate_card_id"
          ).as("rate_card_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.member_id"
          ).as("member_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.is_dw"
          ).as("is_dw"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.pricing_terms"
          ).as("pricing_terms"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.fx_margin_rate_id"
          ).as("fx_margin_rate_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.marketplace_owner_id"
          ).as("marketplace_owner_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.virtual_marketplace_id"
          ).as("virtual_marketplace_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_charges.amino_enabled"
          ).as("amino_enabled")
        ).as("buyer_charges"),
        struct(
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.rate_card_id"
          ).as("rate_card_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.member_id"
          ).as("member_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.is_dw"
          ).as("is_dw"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.pricing_terms"
          ).as("pricing_terms"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.fx_margin_rate_id"
          ).as("fx_margin_rate_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.marketplace_owner_id"
          ).as("marketplace_owner_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.virtual_marketplace_id"
          ).as("virtual_marketplace_id"),
          col(
            "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_charges.amino_enabled"
          ).as("amino_enabled")
        ).as("seller_charges"),
        col(
          "log_impbus_impressions_pricing_dup.impression_event_pricing.buyer_transacted"
        ).as("buyer_transacted"),
        col(
          "log_impbus_impressions_pricing_dup.impression_event_pricing.seller_transacted"
        ).as("seller_transacted")
      ).as("impression_event_pricing"),
      col("log_impbus_impressions_pricing_dup.counterparty_ruleset_type").as(
        "counterparty_ruleset_type"
      ),
      col("log_impbus_impressions_pricing_dup.estimated_audience_imps").as(
        "estimated_audience_imps"
      ),
      col("log_impbus_impressions_pricing_dup.audience_imps").as(
        "audience_imps"
      )
    ).cast(
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
    struct(
      col("log_impbus_impressions_pricing.date_time").as("date_time"),
      col("log_impbus_impressions_pricing.auction_id_64").as("auction_id_64"),
      struct(
        col("log_impbus_impressions_pricing.buyer_charges.rate_card_id").as(
          "rate_card_id"
        ),
        col("log_impbus_impressions_pricing.buyer_charges.member_id").as(
          "member_id"
        ),
        col("log_impbus_impressions_pricing.buyer_charges.is_dw").as("is_dw"),
        col("log_impbus_impressions_pricing.buyer_charges.pricing_terms").as(
          "pricing_terms"
        ),
        col("log_impbus_impressions_pricing.buyer_charges.fx_margin_rate_id")
          .as("fx_margin_rate_id"),
        col("log_impbus_impressions_pricing.buyer_charges.marketplace_owner_id")
          .as("marketplace_owner_id"),
        col(
          "log_impbus_impressions_pricing.buyer_charges.virtual_marketplace_id"
        ).as("virtual_marketplace_id"),
        col("log_impbus_impressions_pricing.buyer_charges.amino_enabled").as(
          "amino_enabled"
        )
      ).as("buyer_charges"),
      struct(
        col("log_impbus_impressions_pricing.seller_charges.rate_card_id").as(
          "rate_card_id"
        ),
        col("log_impbus_impressions_pricing.seller_charges.member_id").as(
          "member_id"
        ),
        col("log_impbus_impressions_pricing.seller_charges.is_dw").as("is_dw"),
        col("log_impbus_impressions_pricing.seller_charges.pricing_terms").as(
          "pricing_terms"
        ),
        col("log_impbus_impressions_pricing.seller_charges.fx_margin_rate_id")
          .as("fx_margin_rate_id"),
        col(
          "log_impbus_impressions_pricing.seller_charges.marketplace_owner_id"
        ).as("marketplace_owner_id"),
        col(
          "log_impbus_impressions_pricing.seller_charges.virtual_marketplace_id"
        ).as("virtual_marketplace_id"),
        col("log_impbus_impressions_pricing.seller_charges.amino_enabled").as(
          "amino_enabled"
        )
      ).as("seller_charges"),
      col("log_impbus_impressions_pricing.buyer_spend").as("buyer_spend"),
      col("log_impbus_impressions_pricing.seller_revenue").as("seller_revenue"),
      col("log_impbus_impressions_pricing.rate_card_auction_type").as(
        "rate_card_auction_type"
      ),
      col("log_impbus_impressions_pricing.rate_card_media_type").as(
        "rate_card_media_type"
      ),
      col("log_impbus_impressions_pricing.direct_clear").as("direct_clear"),
      col("log_impbus_impressions_pricing.auction_timestamp").as(
        "auction_timestamp"
      ),
      col("log_impbus_impressions_pricing.instance_id").as("instance_id"),
      col("log_impbus_impressions_pricing.two_phase_reduction_applied").as(
        "two_phase_reduction_applied"
      ),
      col("log_impbus_impressions_pricing.trade_agreement_id").as(
        "trade_agreement_id"
      ),
      col("log_impbus_impressions_pricing.log_timestamp").as("log_timestamp"),
      struct(
        col(
          "log_impbus_impressions_pricing.trade_agreement_info.applied_term_id"
        ).as("applied_term_id"),
        col(
          "log_impbus_impressions_pricing.trade_agreement_info.applied_term_type"
        ).as("applied_term_type"),
        col(
          "log_impbus_impressions_pricing.trade_agreement_info.targeted_term_ids"
        ).as("targeted_term_ids")
      ).as("trade_agreement_info"),
      col("log_impbus_impressions_pricing.is_buy_it_now").as("is_buy_it_now"),
      col("log_impbus_impressions_pricing.net_buyer_spend").as(
        "net_buyer_spend"
      ),
      struct(
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.gross_payment_value_microcents"
        ).as("gross_payment_value_microcents"),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.net_payment_value_microcents"
        ).as("net_payment_value_microcents"),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
        ).as("seller_revenue_microcents"),
        struct(
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.rate_card_id"
          ).as("rate_card_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.member_id"
          ).as("member_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.is_dw"
          ).as("is_dw"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.pricing_terms"
          ).as("pricing_terms"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.fx_margin_rate_id"
          ).as("fx_margin_rate_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.marketplace_owner_id"
          ).as("marketplace_owner_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.virtual_marketplace_id"
          ).as("virtual_marketplace_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.amino_enabled"
          ).as("amino_enabled")
        ).as("buyer_charges"),
        struct(
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.rate_card_id"
          ).as("rate_card_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.member_id"
          ).as("member_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.is_dw"
          ).as("is_dw"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.pricing_terms"
          ).as("pricing_terms"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.fx_margin_rate_id"
          ).as("fx_margin_rate_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.marketplace_owner_id"
          ).as("marketplace_owner_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.virtual_marketplace_id"
          ).as("virtual_marketplace_id"),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.amino_enabled"
          ).as("amino_enabled")
        ).as("seller_charges"),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_transacted"
        ).as("buyer_transacted"),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_transacted"
        ).as("seller_transacted")
      ).as("impression_event_pricing"),
      col("log_impbus_impressions_pricing.counterparty_ruleset_type").as(
        "counterparty_ruleset_type"
      ),
      col("log_impbus_impressions_pricing.estimated_audience_imps").as(
        "estimated_audience_imps"
      ),
      col("log_impbus_impressions_pricing.audience_imps").as("audience_imps")
    ).cast(
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

}
