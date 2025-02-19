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

object Left_Outer_Join_log_impbus_preeempt {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame = {
    val Config = context.config
    var res    = left.as("left")
    res = res.join(right.as("right"),
                   col("left.auction_id_64") === col("right.auction_id_64"),
                   "outer"
    )
    res.select(
      coalesce(col("left.auction_id_64"),
               col("right.log_impbus_preempt.auction_id_64"),
               col("left.auction_id_64")
      ).as("auction_id_64"),
      coalesce(col("left.date_time"),
               col("right.log_impbus_preempt.date_time"),
               col("left.date_time")
      ).as("date_time"),
      coalesce(
        when(is_not_null(col("left.is_delivered")), col("left.is_delivered")),
        lit(0),
        col("left.is_delivered")
      ).as("is_delivered"),
      coalesce(col("right.log_impbus_preempt.is_dw"), col("left.is_dw"))
        .as("is_dw"),
      col("left.seller_member_id").as("seller_member_id"),
      coalesce(col("right.log_impbus_preempt.buyer_member_id"),
               col("left.buyer_member_id")
      ).as("buyer_member_id"),
      lit(null).cast(IntegerType).as("member_id"),
      col("left.publisher_id").as("publisher_id"),
      col("left.site_id").as("site_id"),
      col("left.tag_id").as("tag_id"),
      lit(null).cast(IntegerType).as("advertiser_id"),
      lit(null).cast(IntegerType).as("campaign_group_id"),
      lit(null).cast(IntegerType).as("campaign_id"),
      lit(null).cast(IntegerType).as("insertion_order_id"),
      coalesce(col("right.log_impbus_preempt.imp_type"),
               col("right.log_impbus_preempt.imp_type"),
               col("left.imp_type"),
               lit(1),
               col("left.imp_type")
      ).as("imp_type"),
      lit(1).as("is_transactable"),
      coalesce(
        when(
          col("right.log_impbus_preempt.date_time") < unix_timestamp(
            date_format(to_timestamp(concat(lit(Config.XR_BUSINESS_DATE),
                                            lit(Config.XR_BUSINESS_HOUR)
                                     ),
                                     "yyyyMMdd_HHmmss"
                        ),
                        "yyyy-MM-dd HH:mm:ss"
            )
          ),
          lit(1)
        ),
        lit(0)
      ).as("is_transacted_previously"),
      coalesce(
        when(
          (col("left.ttl") >= lit(3600)).and(
            unix_timestamp(
              date_format(to_timestamp(concat(lit(Config.XR_BUSINESS_DATE),
                                              lit(Config.XR_BUSINESS_HOUR)
                                       ),
                                       "yyyyMMdd_HHmmss"
                          ),
                          "yyyy-MM-dd HH:mm:ss"
              )
            ) - (lit(4) * lit(3600) + lit(1)) < col("left.date_time")
          ),
          lit(1)
        ),
        lit(0)
      ).as("is_deferred_impression"),
      lit(0).as("has_null_bid"),
      log_impbus_impressions(context).as("log_impbus_impressions"),
      col("right.log_impbus_preempt_count").as("log_impbus_preempt_count"),
      col("right.log_impbus_preempt").as("log_impbus_preempt"),
      col("right.log_impbus_preempt_dup").as("log_impbus_preempt_dup"),
      lit(null).cast(IntegerType).as("log_impbus_impressions_pricing_count"),
      log_impbus_impressions_pricing(context)
        .as("log_impbus_impressions_pricing"),
      log_impbus_impressions_pricing_dup(context).as(
        "log_impbus_impressions_pricing_dup"
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

  def log_impbus_impressions(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("left.date_time").as("date_time"),
      col("left.auction_id_64").as("auction_id_64"),
      col("left.user_id_64").as("user_id_64"),
      col("left.tag_id").as("tag_id"),
      col("left.ip_address").as("ip_address"),
      col("left.venue_id").as("venue_id"),
      col("left.site_domain").as("site_domain"),
      col("left.width").as("width"),
      col("left.height").as("height"),
      col("left.geo_country").as("geo_country"),
      col("left.geo_region").as("geo_region"),
      col("left.gender").as("gender"),
      col("left.age").as("age"),
      col("left.bidder_id").as("bidder_id"),
      col("left.seller_member_id").as("seller_member_id"),
      col("left.buyer_member_id").as("buyer_member_id"),
      col("left.creative_id").as("creative_id"),
      col("left.imp_blacklist_or_fraud").as("imp_blacklist_or_fraud"),
      col("left.imp_bid_on").as("imp_bid_on"),
      col("left.buyer_bid").as("buyer_bid"),
      col("left.buyer_spend").as("buyer_spend"),
      col("left.seller_revenue").as("seller_revenue"),
      col("left.num_of_bids").as("num_of_bids"),
      col("left.ecp").as("ecp"),
      col("left.reserve_price").as("reserve_price"),
      col("left.inv_code").as("inv_code"),
      col("left.call_type").as("call_type"),
      col("left.inventory_source_id").as("inventory_source_id"),
      col("left.cookie_age").as("cookie_age"),
      col("left.brand_id").as("brand_id"),
      col("left.cleared_direct").as("cleared_direct"),
      col("left.forex_allowance").as("forex_allowance"),
      col("left.fold_position").as("fold_position"),
      col("left.external_inv_id").as("external_inv_id"),
      col("left.imp_type").as("imp_type"),
      col("left.is_delivered").as("is_delivered"),
      col("left.is_dw").as("is_dw"),
      col("left.publisher_id").as("publisher_id"),
      col("left.site_id").as("site_id"),
      col("left.content_category_id").as("content_category_id"),
      col("left.datacenter_id").as("datacenter_id"),
      col("left.eap").as("eap"),
      col("left.user_tz_offset").as("user_tz_offset"),
      col("left.user_group_id").as("user_group_id"),
      col("left.pub_rule_id").as("pub_rule_id"),
      col("left.media_type").as("media_type"),
      col("left.operating_system").as("operating_system"),
      col("left.browser").as("browser"),
      col("left.language").as("language"),
      col("left.application_id").as("application_id"),
      col("left.user_locale").as("user_locale"),
      col("left.inventory_url_id").as("inventory_url_id"),
      col("left.audit_type").as("audit_type"),
      col("left.shadow_price").as("shadow_price"),
      col("left.impbus_id").as("impbus_id"),
      col("left.buyer_currency").as("buyer_currency"),
      col("left.buyer_exchange_rate").as("buyer_exchange_rate"),
      col("left.seller_currency").as("seller_currency"),
      col("left.seller_exchange_rate").as("seller_exchange_rate"),
      col("left.vp_expose_domains").as("vp_expose_domains"),
      col("left.vp_expose_categories").as("vp_expose_categories"),
      col("left.vp_expose_pubs").as("vp_expose_pubs"),
      col("left.vp_expose_tag").as("vp_expose_tag"),
      col("left.is_exclusive").as("is_exclusive"),
      col("left.bidder_instance_id").as("bidder_instance_id"),
      col("left.visibility_profile_id").as("visibility_profile_id"),
      col("left.truncate_ip").as("truncate_ip"),
      col("left.device_id").as("device_id"),
      col("left.carrier_id").as("carrier_id"),
      col("left.creative_audit_status").as("creative_audit_status"),
      col("left.is_creative_hosted").as("is_creative_hosted"),
      col("left.city").as("city"),
      col("left.latitude").as("latitude"),
      col("left.longitude").as("longitude"),
      col("left.device_unique_id").as("device_unique_id"),
      col("left.supply_type").as("supply_type"),
      col("left.is_toolbar").as("is_toolbar"),
      col("left.deal_id").as("deal_id"),
      col("left.vp_bitmap").as("vp_bitmap"),
      col("left.ttl").as("ttl"),
      col("left.view_detection_enabled").as("view_detection_enabled"),
      col("left.ozone_id").as("ozone_id"),
      col("left.is_performance").as("is_performance"),
      col("left.sdk_version").as("sdk_version"),
      col("left.inventory_session_frequency").as("inventory_session_frequency"),
      col("left.bid_price_type").as("bid_price_type"),
      col("left.device_type").as("device_type"),
      col("left.dma").as("dma"),
      col("left.postal").as("postal"),
      col("left.package_id").as("package_id"),
      col("left.spend_protection").as("spend_protection"),
      col("left.is_secure").as("is_secure"),
      col("left.estimated_view_rate").as("estimated_view_rate"),
      col("left.external_request_id").as("external_request_id"),
      col("left.viewdef_definition_id_buyer_member")
        .as("viewdef_definition_id_buyer_member"),
      col("left.spend_protection_pixel_id").as("spend_protection_pixel_id"),
      col("left.external_uid").as("external_uid"),
      col("left.request_uuid").as("request_uuid"),
      col("left.mobile_app_instance_id").as("mobile_app_instance_id"),
      col("left.traffic_source_code").as("traffic_source_code"),
      col("left.stitch_group_id").as("stitch_group_id"),
      col("left.deal_type").as("deal_type"),
      col("left.ym_floor_id").as("ym_floor_id"),
      col("left.ym_bias_id").as("ym_bias_id"),
      col("left.estimated_view_rate_over_total")
        .as("estimated_view_rate_over_total"),
      col("left.device_make_id").as("device_make_id"),
      col("left.operating_system_family_id").as("operating_system_family_id"),
      col("left.tag_sizes").as("tag_sizes"),
      col("left.seller_transaction_def").as("seller_transaction_def"),
      col("left.buyer_transaction_def").as("buyer_transaction_def"),
      col("left.predicted_video_view_info").as("predicted_video_view_info"),
      col("left.auction_url").as("auction_url"),
      col("left.allowed_media_types").as("allowed_media_types"),
      col("left.is_imp_rejecter_applied").as("is_imp_rejecter_applied"),
      col("left.imp_rejecter_do_auction").as("imp_rejecter_do_auction"),
      col("left.geo_location").as("geo_location"),
      col("left.seller_bid_currency_conversion_rate")
        .as("seller_bid_currency_conversion_rate"),
      col("left.seller_bid_currency_code").as("seller_bid_currency_code"),
      col("left.is_prebid").as("is_prebid"),
      col("left.default_referrer_url").as("default_referrer_url"),
      col("left.engagement_rates").as("engagement_rates"),
      col("left.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
      col("left.payment_type").as("payment_type"),
      col("left.apply_cost_on_default").as("apply_cost_on_default"),
      col("left.media_buy_cost").as("media_buy_cost"),
      col("left.media_buy_rev_share_pct").as("media_buy_rev_share_pct"),
      col("left.auction_duration_ms").as("auction_duration_ms"),
      col("left.expected_events").as("expected_events"),
      col("left.anonymized_user_info").as("anonymized_user_info"),
      col("left.region_id").as("region_id"),
      col("left.media_company_id").as("media_company_id"),
      col("left.gdpr_consent_cookie").as("gdpr_consent_cookie"),
      col("left.subject_to_gdpr").as("subject_to_gdpr"),
      col("left.browser_code_id").as("browser_code_id"),
      col("left.is_prebid_server_included").as("is_prebid_server_included"),
      col("left.seat_id").as("seat_id"),
      col("left.uid_source").as("uid_source"),
      col("left.is_whiteops_scanned").as("is_whiteops_scanned"),
      col("left.pred_info").as("pred_info"),
      col("left.crossdevice_groups").as("crossdevice_groups"),
      col("left.is_amp").as("is_amp"),
      col("left.hb_source").as("hb_source"),
      col("left.external_campaign_id").as("external_campaign_id"),
      col("left.log_product_ads").as("log_product_ads"),
      col("left.ss_native_assembly_enabled").as("ss_native_assembly_enabled"),
      col("left.emp").as("emp"),
      col("left.personal_identifiers").as("personal_identifiers"),
      col("left.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("left.postal_code_ext_id").as("postal_code_ext_id"),
      col("left.hashed_ip").as("hashed_ip"),
      col("left.external_deal_code").as("external_deal_code"),
      col("left.creative_duration").as("creative_duration"),
      col("left.openrtb_req_subdomain").as("openrtb_req_subdomain"),
      col("left.creative_media_subtype_id").as("creative_media_subtype_id"),
      col("left.is_private_auction").as("is_private_auction"),
      col("left.private_auction_eligible").as("private_auction_eligible"),
      col("left.client_request_id").as("client_request_id"),
      col("left.chrome_traffic_label").as("chrome_traffic_label")
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

}
