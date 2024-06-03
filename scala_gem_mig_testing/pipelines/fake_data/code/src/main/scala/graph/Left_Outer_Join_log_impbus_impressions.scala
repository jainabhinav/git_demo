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

object Left_Outer_Join_log_impbus_impressions {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame,
    in3:     DataFrame,
    in4:     DataFrame,
    in5:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.auction_id_64") === col("in1.auction_id_64"),
            "outer"
      )
      .join(in2.as("in2"),
            col("in1.auction_id_64") === col("in2.auction_id_64"),
            "outer"
      )
      .join(in3.as("in3"),
            col("in2.auction_id_64") === col("in3.auction_id_64"),
            "outer"
      )
      .join(in4.as("in4"),
            col("in3.auction_id_64") === col("in4.auction_id_64"),
            "outer"
      )
      .join(in5.as("in5"),
            col("in4.auction_id_64") === col("in5.auction_id_64"),
            "outer"
      )
      .select(
        coalesce(
          col("in0.auction_id_64"),
          col("in1.auction_id_64"),
          col("in2.auction_id_64"),
          col("in3.log_dw_bid.auction_id_64"),
          col("in4.auction_id_64"),
          col("in5.auction_id_64")
        ).as("auction_id_64"),
        coalesce(col("in0.date_time"),
                 col("in1.date_time"),
                 col("in2.date_time"),
                 col("in3.log_dw_bid.date_time"),
                 col("in4.date_time"),
                 col("in5.date_time")
        ).as("date_time"),
        col("in0.is_delivered").as("is_delivered"),
        col("in0.is_dw").as("is_dw"),
        col("in0.seller_member_id").as("seller_member_id"),
        col("in0.buyer_member_id").as("buyer_member_id"),
        coalesce(col("in0.member_id"), col("in3.log_dw_bid.member_id"))
          .as("member_id"),
        col("in0.publisher_id").as("publisher_id"),
        col("in0.site_id").as("site_id"),
        col("in0.tag_id").as("tag_id"),
        coalesce(col("in0.advertiser_id"), col("in3.log_dw_bid.advertiser_id"))
          .as("advertiser_id"),
        coalesce(col("in0.campaign_group_id"),
                 col("in3.log_dw_bid.campaign_group_id")
        ).as("campaign_group_id"),
        coalesce(col("in0.campaign_id"), col("in3.log_dw_bid.campaign_id"))
          .as("campaign_id"),
        coalesce(col("in0.insertion_order_id"),
                 col("in3.log_dw_bid.insertion_order_id")
        ).as("insertion_order_id"),
        col("in0.imp_type").as("imp_type"),
        col("in0.is_transactable").as("is_transactable"),
        col("in0.is_transacted_previously").as("is_transacted_previously"),
        col("in0.is_deferred_impression").as("is_deferred_impression"),
        col("in0.has_null_bid").as("has_null_bid"),
        coalesce(
          col("in2.additional_clearing_events"),
          col("in4.additional_clearing_events"),
          when(
            is_not_null(col("in2.additional_clearing_events")),
            when(is_not_null(col("in4.additional_clearing_events")),
                 array_union(col("in2.additional_clearing_events"),
                             col("in4.additional_clearing_events")
                 )
            ).otherwise(col("in2.additional_clearing_events"))
          ).when(is_not_null(col("in4.additional_clearing_events")),
                  col("in4.additional_clearing_events")
            )
            .otherwise(lit(null).cast(ArrayType(IntegerType, true)))
        ).as("additional_clearing_events"),
        col("in0.log_impbus_impressions").as("log_impbus_impressions"),
        col("in0.log_impbus_preempt_count").as("log_impbus_preempt_count"),
        col("in0.log_impbus_preempt").as("log_impbus_preempt"),
        col("in0.log_impbus_preempt_dup").as("log_impbus_preempt_dup"),
        col("in0.log_impbus_impressions_pricing_count")
          .as("log_impbus_impressions_pricing_count"),
        col("in0.log_impbus_impressions_pricing")
          .as("log_impbus_impressions_pricing"),
        col("in0.log_impbus_impressions_pricing_dup")
          .as("log_impbus_impressions_pricing_dup"),
        log_impbus_view(context).as("log_impbus_view"),
        col("in2.log_impbus_auction_event").as("log_impbus_auction_event"),
        lit(null).cast(IntegerType).as("log_dw_bid_count"),
        col("in3.log_dw_bid").as("log_dw_bid"),
        log_dw_bid_last(context).as("log_dw_bid_last"),
        col("in3.log_dw_bid_deal").as("log_dw_bid_deal"),
        col("in3.log_dw_bid_curator").as("log_dw_bid_curator"),
        col("in4.log_dw_view").as("log_dw_view"),
        struct(col("in5.date_time").as("date_time"),
               col("in5.auction_id_64").as("auction_id_64"),
               col("in5.video_context").as("video_context")
        ).as("video_slot")
      )

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
    struct(
      col("in1.date_time").as("date_time"),
      col("in1.auction_id_64").as("auction_id_64"),
      col("in1.user_id_64").as("user_id_64"),
      col("in1.view_result").as("view_result"),
      col("in1.ttl").as("ttl"),
      col("in1.view_data").as("view_data"),
      col("in1.viewdef_definition_id").as("viewdef_definition_id"),
      col("in1.viewdef_view_result").as("viewdef_view_result"),
      col("in1.view_not_measurable_type").as("view_not_measurable_type"),
      col("in1.view_not_visible_type").as("view_not_visible_type"),
      col("in1.view_frame_type").as("view_frame_type"),
      col("in1.view_script_version").as("view_script_version"),
      col("in1.view_tag_version").as("view_tag_version"),
      col("in1.view_screen_width").as("view_screen_width"),
      col("in1.view_screen_height").as("view_screen_height"),
      col("in1.view_js_browser").as("view_js_browser"),
      col("in1.view_js_platform").as("view_js_platform"),
      col("in1.view_banner_left").as("view_banner_left"),
      col("in1.view_banner_top").as("view_banner_top"),
      col("in1.view_banner_width").as("view_banner_width"),
      col("in1.view_banner_height").as("view_banner_height"),
      col("in1.view_tracking_duration").as("view_tracking_duration"),
      col("in1.view_page_duration").as("view_page_duration"),
      col("in1.view_usage_duration").as("view_usage_duration"),
      col("in1.view_surface").as("view_surface"),
      col("in1.view_js_message").as("view_js_message"),
      col("in1.view_player_width").as("view_player_width"),
      col("in1.view_player_height").as("view_player_height"),
      col("in1.view_iab_duration").as("view_iab_duration"),
      col("in1.view_iab_inview_count").as("view_iab_inview_count"),
      col("in1.view_duration_gt_0pct").as("view_duration_gt_0pct"),
      col("in1.view_duration_gt_25pct").as("view_duration_gt_25pct"),
      col("in1.view_duration_gt_50pct").as("view_duration_gt_50pct"),
      col("in1.view_duration_gt_75pct").as("view_duration_gt_75pct"),
      col("in1.view_duration_eq_100pct").as("view_duration_eq_100pct"),
      col("in1.auction_timestamp").as("auction_timestamp"),
      col("in1.view_has_banner_left").as("view_has_banner_left"),
      col("in1.view_has_banner_top").as("view_has_banner_top"),
      col("in1.view_mouse_position_final_x").as("view_mouse_position_final_x"),
      col("in1.view_mouse_position_final_y").as("view_mouse_position_final_y"),
      col("in1.view_has_mouse_position_final")
        .as("view_has_mouse_position_final"),
      col("in1.view_mouse_position_initial_x")
        .as("view_mouse_position_initial_x"),
      col("in1.view_mouse_position_initial_y")
        .as("view_mouse_position_initial_y"),
      col("in1.view_has_mouse_position_initial")
        .as("view_has_mouse_position_initial"),
      col("in1.view_mouse_position_page_x").as("view_mouse_position_page_x"),
      col("in1.view_mouse_position_page_y").as("view_mouse_position_page_y"),
      col("in1.view_has_mouse_position_page")
        .as("view_has_mouse_position_page"),
      col("in1.view_mouse_position_timeout_x")
        .as("view_mouse_position_timeout_x"),
      col("in1.view_mouse_position_timeout_y")
        .as("view_mouse_position_timeout_y"),
      col("in1.view_has_mouse_position_timeout")
        .as("view_has_mouse_position_timeout"),
      col("in1.view_session_id").as("view_session_id"),
      col("in1.view_video").as("view_video"),
      col("in1.anonymized_user_info").as("anonymized_user_info"),
      col("in1.is_deferred").as("is_deferred")
    )
  }

}
