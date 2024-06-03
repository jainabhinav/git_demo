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

object Rollup_log_dw_bid {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(
        max(col("date_time")).cast(LongType).as("date_time"),
        lit(null).cast(IntegerType).cast(IntegerType).as("is_delivered"),
        max(lit(0)).cast(IntegerType).as("is_dw"),
        lit(null).cast(IntegerType).cast(IntegerType).as("seller_member_id"),
        lit(null).cast(IntegerType).cast(IntegerType).as("buyer_member_id"),
        max(col("member_id")).cast(IntegerType).as("member_id"),
        lit(null).cast(IntegerType).cast(IntegerType).as("publisher_id"),
        lit(null).cast(IntegerType).cast(IntegerType).as("site_id"),
        lit(null).cast(IntegerType).cast(IntegerType).as("tag_id"),
        max(col("advertiser_id")).cast(IntegerType).as("advertiser_id"),
        max(col("campaign_group_id")).cast(IntegerType).as("campaign_group_id"),
        max(col("campaign_id")).cast(IntegerType).as("campaign_id"),
        max(col("insertion_order_id"))
          .cast(IntegerType)
          .as("insertion_order_id"),
        lit(null).cast(IntegerType).cast(IntegerType).as("imp_type"),
        max(lit(0)).cast(ByteType).as("is_transactable"),
        max(lit(0)).cast(ByteType).as("is_transacted_previously"),
        max(lit(0)).cast(ByteType).as("is_deferred_impression"),
        max(lit(0)).cast(ByteType).as("has_null_bid"),
        lit(null)
          .cast(ArrayType(IntegerType, true))
          .as("additional_clearing_events"),
        log_impbus_impressions(context).as("log_impbus_impressions"),
        lit(null)
          .cast(IntegerType)
          .cast(IntegerType)
          .as("log_impbus_preempt_count"),
        log_impbus_preempt(context).as("log_impbus_preempt"),
        log_impbus_preempt_dup(context).as("log_impbus_preempt_dup"),
        lit(null)
          .cast(IntegerType)
          .cast(IntegerType)
          .as("log_impbus_impressions_pricing_count"),
        log_impbus_impressions_pricing(context)
          .as("log_impbus_impressions_pricing"),
        log_impbus_impressions_pricing_dup(context)
          .as("log_impbus_impressions_pricing_dup"),
        log_impbus_view(context).as("log_impbus_view"),
        log_impbus_auction_event(context).as("log_impbus_auction_event"),
        sum(
          when(
            col("log_type").isNull.or(
              is_not_null(col("log_type").cast(IntegerType))
                .and(col("log_type").cast(IntegerType) =!= lit(6))
                .and(col("log_type").cast(IntegerType) =!= lit(7))
            ),
            lit(1)
          )
        ).cast(IntegerType).as("log_dw_bid_count"),
        log_dw_bid(context).as("log_dw_bid"),
        log_dw_bid_last(context).as("log_dw_bid_last"),
        log_dw_bid_deal(context).as("log_dw_bid_deal"),
        log_dw_bid_curator(context).as("log_dw_bid_curator"),
        log_dw_view(context).as("log_dw_view"),
        lit(null)
          .cast(
            StructType(
              Array(StructField("date_time",     LongType,    true),
                    StructField("auction_id_64", LongType,    true),
                    StructField("video_context", IntegerType, true)
              )
            )
          )
          .as("video_slot")
      )

  def log_dw_bid_deal(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    first(
      when(
        is_not_null(col("log_type").cast(IntegerType))
          .and(col("log_type").cast(IntegerType) === lit(6)),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("price"),
          col("member_id").cast(IntegerType).as("member_id"),
          col("advertiser_id").cast(IntegerType).as("advertiser_id"),
          col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
          col("campaign_id").cast(IntegerType).as("campaign_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("creative_freq").cast(IntegerType).as("creative_freq"),
          col("creative_rec").cast(IntegerType).as("creative_rec"),
          col("advertiser_freq").cast(IntegerType).as("advertiser_freq"),
          col("advertiser_rec").cast(IntegerType).as("advertiser_rec"),
          col("is_remarketing").cast(IntegerType).as("is_remarketing"),
          col("user_group_id").cast(IntegerType).as("user_group_id"),
          col("media_buy_cost"),
          col("is_default").cast(IntegerType).as("is_default"),
          col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
          col("media_buy_rev_share_pct"),
          col("pricing_type"),
          col("can_convert").cast(IntegerType).as("can_convert"),
          col("is_control").cast(IntegerType).as("is_control"),
          col("control_pct"),
          col("control_creative_id")
            .cast(IntegerType)
            .as("control_creative_id"),
          col("cadence_modifier"),
          col("advertiser_currency"),
          col("advertiser_exchange_rate"),
          col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
          col("predict_type").cast(IntegerType).as("predict_type"),
          col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
          col("revenue_value_dollars"),
          col("revenue_value_adv_curr"),
          col("commission_cpm"),
          col("commission_revshare"),
          col("serving_fees_cpm"),
          col("serving_fees_revshare"),
          col("publisher_currency"),
          col("publisher_exchange_rate"),
          col("payment_type").cast(IntegerType).as("payment_type"),
          col("payment_value"),
          col("creative_group_freq")
            .cast(IntegerType)
            .as("creative_group_freq"),
          col("creative_group_rec").cast(IntegerType).as("creative_group_rec"),
          col("revenue_type").cast(IntegerType).as("revenue_type"),
          col("apply_cost_on_default")
            .cast(IntegerType)
            .as("apply_cost_on_default"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
          col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
          col("targeted_segments"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("data_costs"),
          col("targeted_segment_list"),
          col("campaign_group_freq")
            .cast(IntegerType)
            .as("campaign_group_freq"),
          col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
          col("insertion_order_freq")
            .cast(IntegerType)
            .as("insertion_order_freq"),
          col("insertion_order_rec")
            .cast(IntegerType)
            .as("insertion_order_rec"),
          col("buyer_gender"),
          col("buyer_age").cast(IntegerType).as("buyer_age"),
          col("custom_model_id").cast(IntegerType).as("custom_model_id"),
          col("custom_model_last_modified")
            .cast(LongType)
            .as("custom_model_last_modified"),
          col("custom_model_output_code"),
          col("bid_priority").cast(IntegerType).as("bid_priority"),
          col("explore_disposition")
            .cast(IntegerType)
            .as("explore_disposition"),
          col("revenue_auction_event_type")
            .cast(IntegerType)
            .as("revenue_auction_event_type"),
          col("campaign_group_models"),
          col("impression_transaction_type")
            .cast(IntegerType)
            .as("impression_transaction_type"),
          col("is_deferred").cast(IntegerType).as("is_deferred"),
          col("log_type").cast(IntegerType).as("log_type"),
          col("crossdevice_group_anon"),
          col("fx_rate_snapshot_id")
            .cast(IntegerType)
            .as("fx_rate_snapshot_id"),
          col("crossdevice_graph_cost"),
          col("revenue_event_type_id")
            .cast(IntegerType)
            .as("revenue_event_type_id"),
          col("targeted_segment_details"),
          col("insertion_order_budget_interval_id")
            .cast(IntegerType)
            .as("insertion_order_budget_interval_id"),
          col("campaign_group_budget_interval_id")
            .cast(IntegerType)
            .as("campaign_group_budget_interval_id"),
          col("cold_start_price_type")
            .cast(IntegerType)
            .as("cold_start_price_type"),
          col("discovery_state").cast(IntegerType).as("discovery_state"),
          col("revenue_info"),
          col("use_revenue_info").cast(ByteType).as("use_revenue_info"),
          col("sales_tax_rate_pct"),
          col("targeted_crossdevice_graph_id")
            .cast(IntegerType)
            .as("targeted_crossdevice_graph_id"),
          col("product_feed_id").cast(IntegerType).as("product_feed_id"),
          col("item_selection_strategy_id")
            .cast(IntegerType)
            .as("item_selection_strategy_id"),
          col("discovery_prediction"),
          col("bidding_host_id").cast(IntegerType).as("bidding_host_id"),
          col("split_id").cast(IntegerType).as("split_id"),
          col("excluded_targeted_segment_details"),
          col("predicted_kpi_event_rate"),
          col("has_crossdevice_reach_extension")
            .cast(ByteType)
            .as("has_crossdevice_reach_extension"),
          col("advertiser_expected_value_ecpm_ac"),
          col("bpp_multiplier"),
          col("bpp_offset"),
          col("bid_modifier"),
          col("payment_value_microcents")
            .cast(LongType)
            .as("payment_value_microcents"),
          col("crossdevice_graph_membership"),
          col("valuation_landscape"),
          col("line_item_currency"),
          col("measurement_fee_cpm_usd"),
          col("measurement_provider_id")
            .cast(IntegerType)
            .as("measurement_provider_id"),
          col("measurement_provider_member_id")
            .cast(IntegerType)
            .as("measurement_provider_member_id"),
          col("offline_attribution_provider_member_id")
            .cast(IntegerType)
            .as("offline_attribution_provider_member_id"),
          col("offline_attribution_cost_usd_cpm"),
          col("targeted_segment_details_by_id_type"),
          col("offline_attribution"),
          col("frequency_cap_type_internal")
            .cast(IntegerType)
            .as("frequency_cap_type_internal"),
          col("modeled_cap_did_override_line_item_daily_cap")
            .cast(ByteType)
            .as("modeled_cap_did_override_line_item_daily_cap"),
          col("modeled_cap_user_sample_rate"),
          col("bid_rate"),
          col("district_postal_code_lists"),
          col("pre_bpp_price"),
          col("feature_tests_bitmap")
            .cast(IntegerType)
            .as("feature_tests_bitmap")
        )
      )
    )
  }

  def log_impbus_preempt(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
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

  def log_dw_bid_curator(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    first(
      when(
        is_not_null(col("log_type").cast(IntegerType))
          .and(col("log_type").cast(IntegerType) === lit(7)),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("price"),
          col("member_id").cast(IntegerType).as("member_id"),
          col("advertiser_id").cast(IntegerType).as("advertiser_id"),
          col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
          col("campaign_id").cast(IntegerType).as("campaign_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("creative_freq").cast(IntegerType).as("creative_freq"),
          col("creative_rec").cast(IntegerType).as("creative_rec"),
          col("advertiser_freq").cast(IntegerType).as("advertiser_freq"),
          col("advertiser_rec").cast(IntegerType).as("advertiser_rec"),
          col("is_remarketing").cast(IntegerType).as("is_remarketing"),
          col("user_group_id").cast(IntegerType).as("user_group_id"),
          col("media_buy_cost"),
          col("is_default").cast(IntegerType).as("is_default"),
          col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
          col("media_buy_rev_share_pct"),
          col("pricing_type"),
          col("can_convert").cast(IntegerType).as("can_convert"),
          col("is_control").cast(IntegerType).as("is_control"),
          col("control_pct"),
          col("control_creative_id")
            .cast(IntegerType)
            .as("control_creative_id"),
          col("cadence_modifier"),
          col("advertiser_currency"),
          col("advertiser_exchange_rate"),
          col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
          col("predict_type").cast(IntegerType).as("predict_type"),
          col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
          col("revenue_value_dollars"),
          col("revenue_value_adv_curr"),
          col("commission_cpm"),
          col("commission_revshare"),
          col("serving_fees_cpm"),
          col("serving_fees_revshare"),
          col("publisher_currency"),
          col("publisher_exchange_rate"),
          col("payment_type").cast(IntegerType).as("payment_type"),
          col("payment_value"),
          col("creative_group_freq")
            .cast(IntegerType)
            .as("creative_group_freq"),
          col("creative_group_rec").cast(IntegerType).as("creative_group_rec"),
          col("revenue_type").cast(IntegerType).as("revenue_type"),
          col("apply_cost_on_default")
            .cast(IntegerType)
            .as("apply_cost_on_default"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
          col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
          col("targeted_segments"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("data_costs"),
          col("targeted_segment_list"),
          col("campaign_group_freq")
            .cast(IntegerType)
            .as("campaign_group_freq"),
          col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
          col("insertion_order_freq")
            .cast(IntegerType)
            .as("insertion_order_freq"),
          col("insertion_order_rec")
            .cast(IntegerType)
            .as("insertion_order_rec"),
          col("buyer_gender"),
          col("buyer_age").cast(IntegerType).as("buyer_age"),
          col("custom_model_id").cast(IntegerType).as("custom_model_id"),
          col("custom_model_last_modified")
            .cast(LongType)
            .as("custom_model_last_modified"),
          col("custom_model_output_code"),
          col("bid_priority").cast(IntegerType).as("bid_priority"),
          col("explore_disposition")
            .cast(IntegerType)
            .as("explore_disposition"),
          col("revenue_auction_event_type")
            .cast(IntegerType)
            .as("revenue_auction_event_type"),
          col("campaign_group_models"),
          col("impression_transaction_type")
            .cast(IntegerType)
            .as("impression_transaction_type"),
          col("is_deferred").cast(IntegerType).as("is_deferred"),
          col("log_type").cast(IntegerType).as("log_type"),
          col("crossdevice_group_anon"),
          col("fx_rate_snapshot_id")
            .cast(IntegerType)
            .as("fx_rate_snapshot_id"),
          col("crossdevice_graph_cost"),
          col("revenue_event_type_id")
            .cast(IntegerType)
            .as("revenue_event_type_id"),
          col("targeted_segment_details"),
          col("insertion_order_budget_interval_id")
            .cast(IntegerType)
            .as("insertion_order_budget_interval_id"),
          col("campaign_group_budget_interval_id")
            .cast(IntegerType)
            .as("campaign_group_budget_interval_id"),
          col("cold_start_price_type")
            .cast(IntegerType)
            .as("cold_start_price_type"),
          col("discovery_state").cast(IntegerType).as("discovery_state"),
          col("revenue_info"),
          col("use_revenue_info").cast(ByteType).as("use_revenue_info"),
          col("sales_tax_rate_pct"),
          col("targeted_crossdevice_graph_id")
            .cast(IntegerType)
            .as("targeted_crossdevice_graph_id"),
          col("product_feed_id").cast(IntegerType).as("product_feed_id"),
          col("item_selection_strategy_id")
            .cast(IntegerType)
            .as("item_selection_strategy_id"),
          col("discovery_prediction"),
          col("bidding_host_id").cast(IntegerType).as("bidding_host_id"),
          col("split_id").cast(IntegerType).as("split_id"),
          col("excluded_targeted_segment_details"),
          col("predicted_kpi_event_rate"),
          col("has_crossdevice_reach_extension")
            .cast(ByteType)
            .as("has_crossdevice_reach_extension"),
          col("advertiser_expected_value_ecpm_ac"),
          col("bpp_multiplier"),
          col("bpp_offset"),
          col("bid_modifier"),
          col("payment_value_microcents")
            .cast(LongType)
            .as("payment_value_microcents"),
          col("crossdevice_graph_membership"),
          col("valuation_landscape"),
          col("line_item_currency"),
          col("measurement_fee_cpm_usd"),
          col("measurement_provider_id")
            .cast(IntegerType)
            .as("measurement_provider_id"),
          col("measurement_provider_member_id")
            .cast(IntegerType)
            .as("measurement_provider_member_id"),
          col("offline_attribution_provider_member_id")
            .cast(IntegerType)
            .as("offline_attribution_provider_member_id"),
          col("offline_attribution_cost_usd_cpm"),
          col("targeted_segment_details_by_id_type"),
          col("offline_attribution"),
          col("frequency_cap_type_internal")
            .cast(IntegerType)
            .as("frequency_cap_type_internal"),
          col("modeled_cap_did_override_line_item_daily_cap")
            .cast(ByteType)
            .as("modeled_cap_did_override_line_item_daily_cap"),
          col("modeled_cap_user_sample_rate"),
          col("bid_rate"),
          col("district_postal_code_lists"),
          col("pre_bpp_price"),
          col("feature_tests_bitmap")
            .cast(IntegerType)
            .as("feature_tests_bitmap")
        )
      )
    )
  }

  def log_impbus_auction_event(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                 LongType,    true),
          StructField("auction_id_64",             LongType,    true),
          StructField("payment_value_microcents",  LongType,    true),
          StructField("transaction_event",         IntegerType, true),
          StructField("transaction_event_type_id", IntegerType, true),
          StructField("is_deferred",               ByteType,    true),
          StructField(
            "auction_event_pricing",
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
          )
        )
      )
    )
  }

  def log_dw_bid(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    first(
      when(
        col("log_type").isNull.or(
          is_not_null(col("log_type").cast(IntegerType))
            .and(col("log_type").cast(IntegerType) =!= lit(6))
            .and(col("log_type").cast(IntegerType) =!= lit(7))
        ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("price"),
          col("member_id").cast(IntegerType).as("member_id"),
          col("advertiser_id").cast(IntegerType).as("advertiser_id"),
          col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
          col("campaign_id").cast(IntegerType).as("campaign_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("creative_freq").cast(IntegerType).as("creative_freq"),
          col("creative_rec").cast(IntegerType).as("creative_rec"),
          col("advertiser_freq").cast(IntegerType).as("advertiser_freq"),
          col("advertiser_rec").cast(IntegerType).as("advertiser_rec"),
          col("is_remarketing").cast(IntegerType).as("is_remarketing"),
          col("user_group_id").cast(IntegerType).as("user_group_id"),
          col("media_buy_cost"),
          col("is_default").cast(IntegerType).as("is_default"),
          col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
          col("media_buy_rev_share_pct"),
          col("pricing_type"),
          col("can_convert").cast(IntegerType).as("can_convert"),
          col("is_control").cast(IntegerType).as("is_control"),
          col("control_pct"),
          col("control_creative_id")
            .cast(IntegerType)
            .as("control_creative_id"),
          col("cadence_modifier"),
          col("advertiser_currency"),
          col("advertiser_exchange_rate"),
          col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
          col("predict_type").cast(IntegerType).as("predict_type"),
          col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
          col("revenue_value_dollars"),
          col("revenue_value_adv_curr"),
          col("commission_cpm"),
          col("commission_revshare"),
          col("serving_fees_cpm"),
          col("serving_fees_revshare"),
          col("publisher_currency"),
          col("publisher_exchange_rate"),
          col("payment_type").cast(IntegerType).as("payment_type"),
          col("payment_value"),
          col("creative_group_freq")
            .cast(IntegerType)
            .as("creative_group_freq"),
          col("creative_group_rec").cast(IntegerType).as("creative_group_rec"),
          col("revenue_type").cast(IntegerType).as("revenue_type"),
          col("apply_cost_on_default")
            .cast(IntegerType)
            .as("apply_cost_on_default"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
          col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
          col("targeted_segments"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("data_costs"),
          col("targeted_segment_list"),
          col("campaign_group_freq")
            .cast(IntegerType)
            .as("campaign_group_freq"),
          col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
          col("insertion_order_freq")
            .cast(IntegerType)
            .as("insertion_order_freq"),
          col("insertion_order_rec")
            .cast(IntegerType)
            .as("insertion_order_rec"),
          col("buyer_gender"),
          col("buyer_age").cast(IntegerType).as("buyer_age"),
          col("custom_model_id").cast(IntegerType).as("custom_model_id"),
          col("custom_model_last_modified")
            .cast(LongType)
            .as("custom_model_last_modified"),
          col("custom_model_output_code"),
          col("bid_priority").cast(IntegerType).as("bid_priority"),
          col("explore_disposition")
            .cast(IntegerType)
            .as("explore_disposition"),
          col("revenue_auction_event_type")
            .cast(IntegerType)
            .as("revenue_auction_event_type"),
          col("campaign_group_models"),
          col("impression_transaction_type")
            .cast(IntegerType)
            .as("impression_transaction_type"),
          col("is_deferred").cast(IntegerType).as("is_deferred"),
          col("log_type").cast(IntegerType).as("log_type"),
          col("crossdevice_group_anon"),
          col("fx_rate_snapshot_id")
            .cast(IntegerType)
            .as("fx_rate_snapshot_id"),
          col("crossdevice_graph_cost"),
          col("revenue_event_type_id")
            .cast(IntegerType)
            .as("revenue_event_type_id"),
          col("targeted_segment_details"),
          col("insertion_order_budget_interval_id")
            .cast(IntegerType)
            .as("insertion_order_budget_interval_id"),
          col("campaign_group_budget_interval_id")
            .cast(IntegerType)
            .as("campaign_group_budget_interval_id"),
          col("cold_start_price_type")
            .cast(IntegerType)
            .as("cold_start_price_type"),
          col("discovery_state").cast(IntegerType).as("discovery_state"),
          col("revenue_info"),
          col("use_revenue_info").cast(ByteType).as("use_revenue_info"),
          col("sales_tax_rate_pct"),
          col("targeted_crossdevice_graph_id")
            .cast(IntegerType)
            .as("targeted_crossdevice_graph_id"),
          col("product_feed_id").cast(IntegerType).as("product_feed_id"),
          col("item_selection_strategy_id")
            .cast(IntegerType)
            .as("item_selection_strategy_id"),
          col("discovery_prediction"),
          col("bidding_host_id").cast(IntegerType).as("bidding_host_id"),
          col("split_id").cast(IntegerType).as("split_id"),
          col("excluded_targeted_segment_details"),
          col("predicted_kpi_event_rate"),
          col("has_crossdevice_reach_extension")
            .cast(ByteType)
            .as("has_crossdevice_reach_extension"),
          col("advertiser_expected_value_ecpm_ac"),
          col("bpp_multiplier"),
          col("bpp_offset"),
          col("bid_modifier"),
          col("payment_value_microcents")
            .cast(LongType)
            .as("payment_value_microcents"),
          col("crossdevice_graph_membership"),
          col("valuation_landscape"),
          col("line_item_currency"),
          col("measurement_fee_cpm_usd"),
          col("measurement_provider_id")
            .cast(IntegerType)
            .as("measurement_provider_id"),
          col("measurement_provider_member_id")
            .cast(IntegerType)
            .as("measurement_provider_member_id"),
          col("offline_attribution_provider_member_id")
            .cast(IntegerType)
            .as("offline_attribution_provider_member_id"),
          col("offline_attribution_cost_usd_cpm"),
          col("targeted_segment_details_by_id_type"),
          col("offline_attribution"),
          col("frequency_cap_type_internal")
            .cast(IntegerType)
            .as("frequency_cap_type_internal"),
          col("modeled_cap_did_override_line_item_daily_cap")
            .cast(ByteType)
            .as("modeled_cap_did_override_line_item_daily_cap"),
          col("modeled_cap_user_sample_rate"),
          col("bid_rate"),
          col("district_postal_code_lists"),
          col("pre_bpp_price"),
          col("feature_tests_bitmap")
            .cast(IntegerType)
            .as("feature_tests_bitmap")
        )
      )
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

  def log_dw_view(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("user_id_64",               LongType,    true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("booked_revenue_dollars",   DoubleType,  true),
          StructField("booked_revenue_adv_curr",  DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_value",            DoubleType,  true),
          StructField("ip_address",               StringType,  true),
          StructField("auction_timestamp",        LongType,    true),
          StructField("view_auction_event_type",  IntegerType, true),
          StructField(
            "anonymized_user_info",
            StructType(Array(StructField("user_id", BinaryType, true))),
            true
          ),
          StructField("view_event_type_id", IntegerType, true),
          StructField(
            "revenue_info",
            StructType(
              Array(
                StructField("total_partner_fees_microcents", LongType,   true),
                StructField("booked_revenue_dollars",        DoubleType, true),
                StructField("booked_revenue_adv_curr",       DoubleType, true),
                StructField("total_data_costs_microcents",   LongType,   true),
                StructField("total_profit_microcents",       LongType,   true),
                StructField("total_segment_data_costs_microcents",
                            LongType,
                            true
                ),
                StructField("total_feature_costs_microcents", LongType, true)
              )
            ),
            true
          ),
          StructField("use_revenue_info",     ByteType,   true),
          StructField("is_deferred",          ByteType,   true),
          StructField("ecpm_conversion_rate", DoubleType, true)
        )
      )
    )
  }

  def log_impbus_preempt_dup(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
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

  def log_impbus_view(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                       LongType,    true),
          StructField("auction_id_64",                   LongType,    true),
          StructField("user_id_64",                      LongType,    true),
          StructField("view_result",                     IntegerType, true),
          StructField("ttl",                             IntegerType, true),
          StructField("view_data",                       StringType,  true),
          StructField("viewdef_definition_id",           IntegerType, true),
          StructField("viewdef_view_result",             IntegerType, true),
          StructField("view_not_measurable_type",        IntegerType, true),
          StructField("view_not_visible_type",           IntegerType, true),
          StructField("view_frame_type",                 IntegerType, true),
          StructField("view_script_version",             IntegerType, true),
          StructField("view_tag_version",                StringType,  true),
          StructField("view_screen_width",               IntegerType, true),
          StructField("view_screen_height",              IntegerType, true),
          StructField("view_js_browser",                 StringType,  true),
          StructField("view_js_platform",                StringType,  true),
          StructField("view_banner_left",                IntegerType, true),
          StructField("view_banner_top",                 IntegerType, true),
          StructField("view_banner_width",               IntegerType, true),
          StructField("view_banner_height",              IntegerType, true),
          StructField("view_tracking_duration",          DoubleType,  true),
          StructField("view_page_duration",              DoubleType,  true),
          StructField("view_usage_duration",             DoubleType,  true),
          StructField("view_surface",                    DoubleType,  true),
          StructField("view_js_message",                 StringType,  true),
          StructField("view_player_width",               IntegerType, true),
          StructField("view_player_height",              IntegerType, true),
          StructField("view_iab_duration",               DoubleType,  true),
          StructField("view_iab_inview_count",           IntegerType, true),
          StructField("view_duration_gt_0pct",           DoubleType,  true),
          StructField("view_duration_gt_25pct",          DoubleType,  true),
          StructField("view_duration_gt_50pct",          DoubleType,  true),
          StructField("view_duration_gt_75pct",          DoubleType,  true),
          StructField("view_duration_eq_100pct",         DoubleType,  true),
          StructField("auction_timestamp",               LongType,    true),
          StructField("view_has_banner_left",            IntegerType, true),
          StructField("view_has_banner_top",             IntegerType, true),
          StructField("view_mouse_position_final_x",     IntegerType, true),
          StructField("view_mouse_position_final_y",     IntegerType, true),
          StructField("view_has_mouse_position_final",   IntegerType, true),
          StructField("view_mouse_position_initial_x",   IntegerType, true),
          StructField("view_mouse_position_initial_y",   IntegerType, true),
          StructField("view_has_mouse_position_initial", IntegerType, true),
          StructField("view_mouse_position_page_x",      IntegerType, true),
          StructField("view_mouse_position_page_y",      IntegerType, true),
          StructField("view_has_mouse_position_page",    IntegerType, true),
          StructField("view_mouse_position_timeout_x",   IntegerType, true),
          StructField("view_mouse_position_timeout_y",   IntegerType, true),
          StructField("view_has_mouse_position_timeout", IntegerType, true),
          StructField("view_session_id",                 LongType,    true),
          StructField(
            "view_video",
            StructType(
              Array(
                StructField("view_audio_duration_eq_100pct", DoubleType, true),
                StructField("view_creative_duration",        DoubleType, true)
              )
            ),
            true
          ),
          StructField(
            "anonymized_user_info",
            StructType(Array(StructField("user_id", BinaryType, true))),
            true
          ),
          StructField("is_deferred", ByteType, true)
        )
      )
    )
  }

  def log_dw_bid_last(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    last(
      when(
        col("log_type").isNull.or(
          is_not_null(col("log_type").cast(IntegerType))
            .and(col("log_type").cast(IntegerType) =!= lit(6))
            .and(col("log_type").cast(IntegerType) =!= lit(7))
        ),
        struct(
          col("date_time").cast(LongType).as("date_time"),
          col("auction_id_64").cast(LongType).as("auction_id_64"),
          col("price"),
          col("member_id").cast(IntegerType).as("member_id"),
          col("advertiser_id").cast(IntegerType).as("advertiser_id"),
          col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
          col("campaign_id").cast(IntegerType).as("campaign_id"),
          col("creative_id").cast(IntegerType).as("creative_id"),
          col("creative_freq").cast(IntegerType).as("creative_freq"),
          col("creative_rec").cast(IntegerType).as("creative_rec"),
          col("advertiser_freq").cast(IntegerType).as("advertiser_freq"),
          col("advertiser_rec").cast(IntegerType).as("advertiser_rec"),
          col("is_remarketing").cast(IntegerType).as("is_remarketing"),
          col("user_group_id").cast(IntegerType).as("user_group_id"),
          col("media_buy_cost"),
          col("is_default").cast(IntegerType).as("is_default"),
          col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
          col("media_buy_rev_share_pct"),
          col("pricing_type"),
          col("can_convert").cast(IntegerType).as("can_convert"),
          col("is_control").cast(IntegerType).as("is_control"),
          col("control_pct"),
          col("control_creative_id")
            .cast(IntegerType)
            .as("control_creative_id"),
          col("cadence_modifier"),
          col("advertiser_currency"),
          col("advertiser_exchange_rate"),
          col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
          col("predict_type").cast(IntegerType).as("predict_type"),
          col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
          col("revenue_value_dollars"),
          col("revenue_value_adv_curr"),
          col("commission_cpm"),
          col("commission_revshare"),
          col("serving_fees_cpm"),
          col("serving_fees_revshare"),
          col("publisher_currency"),
          col("publisher_exchange_rate"),
          col("payment_type").cast(IntegerType).as("payment_type"),
          col("payment_value"),
          col("creative_group_freq")
            .cast(IntegerType)
            .as("creative_group_freq"),
          col("creative_group_rec").cast(IntegerType).as("creative_group_rec"),
          col("revenue_type").cast(IntegerType).as("revenue_type"),
          col("apply_cost_on_default")
            .cast(IntegerType)
            .as("apply_cost_on_default"),
          col("instance_id").cast(IntegerType).as("instance_id"),
          col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
          col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
          col("targeted_segments"),
          col("ttl").cast(IntegerType).as("ttl"),
          col("auction_timestamp").cast(LongType).as("auction_timestamp"),
          col("data_costs"),
          col("targeted_segment_list"),
          col("campaign_group_freq")
            .cast(IntegerType)
            .as("campaign_group_freq"),
          col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
          col("insertion_order_freq")
            .cast(IntegerType)
            .as("insertion_order_freq"),
          col("insertion_order_rec")
            .cast(IntegerType)
            .as("insertion_order_rec"),
          col("buyer_gender"),
          col("buyer_age").cast(IntegerType).as("buyer_age"),
          col("custom_model_id").cast(IntegerType).as("custom_model_id"),
          col("custom_model_last_modified")
            .cast(LongType)
            .as("custom_model_last_modified"),
          col("custom_model_output_code"),
          col("bid_priority").cast(IntegerType).as("bid_priority"),
          col("explore_disposition")
            .cast(IntegerType)
            .as("explore_disposition"),
          col("revenue_auction_event_type")
            .cast(IntegerType)
            .as("revenue_auction_event_type"),
          col("campaign_group_models"),
          col("impression_transaction_type")
            .cast(IntegerType)
            .as("impression_transaction_type"),
          col("is_deferred").cast(IntegerType).as("is_deferred"),
          col("log_type").cast(IntegerType).as("log_type"),
          col("crossdevice_group_anon"),
          col("fx_rate_snapshot_id")
            .cast(IntegerType)
            .as("fx_rate_snapshot_id"),
          col("crossdevice_graph_cost"),
          col("revenue_event_type_id")
            .cast(IntegerType)
            .as("revenue_event_type_id"),
          col("targeted_segment_details"),
          col("insertion_order_budget_interval_id")
            .cast(IntegerType)
            .as("insertion_order_budget_interval_id"),
          col("campaign_group_budget_interval_id")
            .cast(IntegerType)
            .as("campaign_group_budget_interval_id"),
          col("cold_start_price_type")
            .cast(IntegerType)
            .as("cold_start_price_type"),
          col("discovery_state").cast(IntegerType).as("discovery_state"),
          col("revenue_info"),
          col("use_revenue_info").cast(ByteType).as("use_revenue_info"),
          col("sales_tax_rate_pct"),
          col("targeted_crossdevice_graph_id")
            .cast(IntegerType)
            .as("targeted_crossdevice_graph_id"),
          col("product_feed_id").cast(IntegerType).as("product_feed_id"),
          col("item_selection_strategy_id")
            .cast(IntegerType)
            .as("item_selection_strategy_id"),
          col("discovery_prediction"),
          col("bidding_host_id").cast(IntegerType).as("bidding_host_id"),
          col("split_id").cast(IntegerType).as("split_id"),
          col("excluded_targeted_segment_details"),
          col("predicted_kpi_event_rate"),
          col("has_crossdevice_reach_extension")
            .cast(ByteType)
            .as("has_crossdevice_reach_extension"),
          col("advertiser_expected_value_ecpm_ac"),
          col("bpp_multiplier"),
          col("bpp_offset"),
          col("bid_modifier"),
          col("payment_value_microcents")
            .cast(LongType)
            .as("payment_value_microcents"),
          col("crossdevice_graph_membership"),
          col("valuation_landscape"),
          col("line_item_currency"),
          col("measurement_fee_cpm_usd"),
          col("measurement_provider_id")
            .cast(IntegerType)
            .as("measurement_provider_id"),
          col("measurement_provider_member_id")
            .cast(IntegerType)
            .as("measurement_provider_member_id"),
          col("offline_attribution_provider_member_id")
            .cast(IntegerType)
            .as("offline_attribution_provider_member_id"),
          col("offline_attribution_cost_usd_cpm"),
          col("targeted_segment_details_by_id_type"),
          col("offline_attribution"),
          col("frequency_cap_type_internal")
            .cast(IntegerType)
            .as("frequency_cap_type_internal"),
          col("modeled_cap_did_override_line_item_daily_cap")
            .cast(ByteType)
            .as("modeled_cap_did_override_line_item_daily_cap"),
          col("modeled_cap_user_sample_rate"),
          col("bid_rate"),
          col("district_postal_code_lists"),
          col("pre_bpp_price"),
          col("feature_tests_bitmap")
            .cast(IntegerType)
            .as("feature_tests_bitmap")
        )
      )
    )
  }

}
