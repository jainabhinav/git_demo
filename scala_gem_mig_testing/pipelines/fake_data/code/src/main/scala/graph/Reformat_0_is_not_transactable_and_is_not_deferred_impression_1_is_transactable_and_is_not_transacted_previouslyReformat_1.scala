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

object Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("date_time").cast(LongType).as("date_time"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("member_id").cast(IntegerType).as("member_id"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("is_transactable").cast(ByteType).as("is_transactable"),
      col("is_transacted_previously")
        .cast(ByteType)
        .as("is_transacted_previously"),
      col("is_deferred_impression").cast(ByteType).as("is_deferred_impression"),
      col("has_null_bid").cast(ByteType).as("has_null_bid"),
      lit(null)
        .cast(ArrayType(IntegerType, true))
        .as("additional_clearing_events"),
      col("log_impbus_impressions"),
      col("log_impbus_preempt_count")
        .cast(IntegerType)
        .as("log_impbus_preempt_count"),
      col("log_impbus_preempt"),
      col("log_impbus_preempt_dup"),
      col("log_impbus_impressions_pricing_count")
        .cast(IntegerType)
        .as("log_impbus_impressions_pricing_count"),
      col("log_impbus_impressions_pricing"),
      col("log_impbus_impressions_pricing_dup"),
      log_impbus_view(context).as("log_impbus_view"),
      log_impbus_auction_event(context).as("log_impbus_auction_event"),
      lit(null).cast(IntegerType).as("log_dw_bid_count"),
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

  def log_dw_bid_last(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
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
          StructField("use_revenue_info",              ByteType,    true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType, true),
          StructField("has_crossdevice_reach_extension",   ByteType,   true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType, true),
          StructField("bpp_multiplier",                    DoubleType, true),
          StructField("bpp_offset",                        DoubleType, true),
          StructField("bid_modifier",                      DoubleType, true),
          StructField("payment_value_microcents",          LongType,   true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      ByteType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
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

  def log_dw_bid(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
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
          StructField("use_revenue_info",              ByteType,    true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType, true),
          StructField("has_crossdevice_reach_extension",   ByteType,   true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType, true),
          StructField("bpp_multiplier",                    DoubleType, true),
          StructField("bpp_offset",                        DoubleType, true),
          StructField("bid_modifier",                      DoubleType, true),
          StructField("payment_value_microcents",          LongType,   true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      ByteType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
        )
      )
    )
  }

  def log_dw_bid_curator(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
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
          StructField("use_revenue_info",              ByteType,    true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType, true),
          StructField("has_crossdevice_reach_extension",   ByteType,   true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType, true),
          StructField("bpp_multiplier",                    DoubleType, true),
          StructField("bpp_offset",                        DoubleType, true),
          StructField("bid_modifier",                      DoubleType, true),
          StructField("payment_value_microcents",          LongType,   true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      ByteType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
        )
      )
    )
  }

  def log_dw_bid_deal(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      StructType(
        Array(
          StructField("date_time",                LongType,    true),
          StructField("auction_id_64",            LongType,    true),
          StructField("price",                    DoubleType,  true),
          StructField("member_id",                IntegerType, true),
          StructField("advertiser_id",            IntegerType, true),
          StructField("campaign_group_id",        IntegerType, true),
          StructField("campaign_id",              IntegerType, true),
          StructField("creative_id",              IntegerType, true),
          StructField("creative_freq",            IntegerType, true),
          StructField("creative_rec",             IntegerType, true),
          StructField("advertiser_freq",          IntegerType, true),
          StructField("advertiser_rec",           IntegerType, true),
          StructField("is_remarketing",           IntegerType, true),
          StructField("user_group_id",            IntegerType, true),
          StructField("media_buy_cost",           DoubleType,  true),
          StructField("is_default",               IntegerType, true),
          StructField("pub_rule_id",              IntegerType, true),
          StructField("media_buy_rev_share_pct",  DoubleType,  true),
          StructField("pricing_type",             StringType,  true),
          StructField("can_convert",              IntegerType, true),
          StructField("is_control",               IntegerType, true),
          StructField("control_pct",              DoubleType,  true),
          StructField("control_creative_id",      IntegerType, true),
          StructField("cadence_modifier",         DoubleType,  true),
          StructField("advertiser_currency",      StringType,  true),
          StructField("advertiser_exchange_rate", DoubleType,  true),
          StructField("insertion_order_id",       IntegerType, true),
          StructField("predict_type",             IntegerType, true),
          StructField("predict_type_goal",        IntegerType, true),
          StructField("revenue_value_dollars",    DoubleType,  true),
          StructField("revenue_value_adv_curr",   DoubleType,  true),
          StructField("commission_cpm",           DoubleType,  true),
          StructField("commission_revshare",      DoubleType,  true),
          StructField("serving_fees_cpm",         DoubleType,  true),
          StructField("serving_fees_revshare",    DoubleType,  true),
          StructField("publisher_currency",       StringType,  true),
          StructField("publisher_exchange_rate",  DoubleType,  true),
          StructField("payment_type",             IntegerType, true),
          StructField("payment_value",            DoubleType,  true),
          StructField("creative_group_freq",      IntegerType, true),
          StructField("creative_group_rec",       IntegerType, true),
          StructField("revenue_type",             IntegerType, true),
          StructField("apply_cost_on_default",    IntegerType, true),
          StructField("instance_id",              IntegerType, true),
          StructField("vp_expose_age",            IntegerType, true),
          StructField("vp_expose_gender",         IntegerType, true),
          StructField("targeted_segments",        StringType,  true),
          StructField("ttl",                      IntegerType, true),
          StructField("auction_timestamp",        LongType,    true),
          StructField(
            "data_costs",
            ArrayType(
              StructType(
                Array(
                  StructField("data_member_id", IntegerType, true),
                  StructField("cost",           DoubleType,  true),
                  StructField("used_segments",
                              ArrayType(IntegerType, true),
                              true
                  ),
                  StructField("cost_pct", DoubleType, true)
                )
              ),
              true
            ),
            true
          ),
          StructField("targeted_segment_list",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("campaign_group_freq",        IntegerType, true),
          StructField("campaign_group_rec",         IntegerType, true),
          StructField("insertion_order_freq",       IntegerType, true),
          StructField("insertion_order_rec",        IntegerType, true),
          StructField("buyer_gender",               StringType,  true),
          StructField("buyer_age",                  IntegerType, true),
          StructField("custom_model_id",            IntegerType, true),
          StructField("custom_model_last_modified", LongType,    true),
          StructField("custom_model_output_code",   StringType,  true),
          StructField("bid_priority",               IntegerType, true),
          StructField("explore_disposition",        IntegerType, true),
          StructField("revenue_auction_event_type", IntegerType, true),
          StructField(
            "campaign_group_models",
            ArrayType(
              StructType(
                Array(
                  StructField("model_type", IntegerType, true),
                  StructField("model_id",   IntegerType, true),
                  StructField("leaf_code",  StringType,  true),
                  StructField("origin",     IntegerType, true),
                  StructField("experiment", IntegerType, true),
                  StructField("value",      DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("impression_transaction_type", IntegerType, true),
          StructField("is_deferred",                 IntegerType, true),
          StructField("log_type",                    IntegerType, true),
          StructField("crossdevice_group_anon",
                      StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
          ),
          StructField("fx_rate_snapshot_id", IntegerType, true),
          StructField(
            "crossdevice_graph_cost",
            StructType(
              Array(StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
              )
            ),
            true
          ),
          StructField("revenue_event_type_id", IntegerType, true),
          StructField(
            "targeted_segment_details",
            ArrayType(StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField("insertion_order_budget_interval_id", IntegerType, true),
          StructField("campaign_group_budget_interval_id",  IntegerType, true),
          StructField("cold_start_price_type",              IntegerType, true),
          StructField("discovery_state",                    IntegerType, true),
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
          StructField("use_revenue_info",              ByteType,    true),
          StructField("sales_tax_rate_pct",            DoubleType,  true),
          StructField("targeted_crossdevice_graph_id", IntegerType, true),
          StructField("product_feed_id",               IntegerType, true),
          StructField("item_selection_strategy_id",    IntegerType, true),
          StructField("discovery_prediction",          DoubleType,  true),
          StructField("bidding_host_id",               IntegerType, true),
          StructField("split_id",                      IntegerType, true),
          StructField(
            "excluded_targeted_segment_details",
            ArrayType(
              StructType(Array(StructField("segment_id", IntegerType, true))),
              true
            ),
            true
          ),
          StructField("predicted_kpi_event_rate",          DoubleType, true),
          StructField("has_crossdevice_reach_extension",   ByteType,   true),
          StructField("advertiser_expected_value_ecpm_ac", DoubleType, true),
          StructField("bpp_multiplier",                    DoubleType, true),
          StructField("bpp_offset",                        DoubleType, true),
          StructField("bid_modifier",                      DoubleType, true),
          StructField("payment_value_microcents",          LongType,   true),
          StructField(
            "crossdevice_graph_membership",
            ArrayType(StructType(
                        Array(StructField("graph_id", IntegerType, true),
                              StructField("group_id", BinaryType,  true)
                        )
                      ),
                      true
            ),
            true
          ),
          StructField(
            "valuation_landscape",
            ArrayType(
              StructType(
                Array(
                  StructField("kpi_event_id",               IntegerType, true),
                  StructField("ev_kpi_event_ac",            DoubleType,  true),
                  StructField("p_kpi_event",                DoubleType,  true),
                  StructField("bpo_aggressiveness_factor",  DoubleType,  true),
                  StructField("min_margin_pct",             DoubleType,  true),
                  StructField("max_revenue_or_bid_value",   DoubleType,  true),
                  StructField("min_revenue_or_bid_value",   DoubleType,  true),
                  StructField("cold_start_price_ac",        DoubleType,  true),
                  StructField("dynamic_bid_max_revenue_ac", DoubleType,  true),
                  StructField("p_revenue_event",            DoubleType,  true),
                  StructField("total_fees_deducted_ac",     DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("line_item_currency",             StringType,  true),
          StructField("measurement_fee_cpm_usd",        DoubleType,  true),
          StructField("measurement_provider_id",        IntegerType, true),
          StructField("measurement_provider_member_id", IntegerType, true),
          StructField("offline_attribution_provider_member_id",
                      IntegerType,
                      true
          ),
          StructField("offline_attribution_cost_usd_cpm", DoubleType, true),
          StructField(
            "targeted_segment_details_by_id_type",
            ArrayType(
              StructType(
                Array(
                  StructField("identity_type", IntegerType, true),
                  StructField(
                    "targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                        )
                      ),
                      true
                    ),
                    true
                  )
                )
              ),
              true
            ),
            true
          ),
          StructField(
            "offline_attribution",
            ArrayType(
              StructType(
                Array(StructField("provider_member_id", IntegerType, true),
                      StructField("cost_usd_cpm",       DoubleType,  true)
                )
              ),
              true
            ),
            true
          ),
          StructField("frequency_cap_type_internal", IntegerType, true),
          StructField("modeled_cap_did_override_line_item_daily_cap",
                      ByteType,
                      true
          ),
          StructField("modeled_cap_user_sample_rate", DoubleType, true),
          StructField("bid_rate",                     DoubleType, true),
          StructField("district_postal_code_lists",
                      ArrayType(IntegerType, true),
                      true
          ),
          StructField("pre_bpp_price",        DoubleType,  true),
          StructField("feature_tests_bitmap", IntegerType, true)
        )
      )
    )
  }

}
