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

object Reformat_select_log_dw_bid {

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
      advertiser_id(context).as("advertiser_id"),
      campaign_group_id(context).as("campaign_group_id"),
      campaign_id(context).as("campaign_id"),
      insertion_order_id(context).as("insertion_order_id"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("is_transactable").cast(ByteType).as("is_transactable"),
      col("is_transacted_previously")
        .cast(ByteType)
        .as("is_transacted_previously"),
      col("is_deferred_impression").cast(ByteType).as("is_deferred_impression"),
      col("has_null_bid").cast(ByteType).as("has_null_bid"),
      col("additional_clearing_events"),
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
      col("log_impbus_view"),
      col("log_impbus_auction_event"),
      col("log_dw_bid_count").cast(IntegerType).as("log_dw_bid_count"),
      log_dw_bid(context).as("log_dw_bid"),
      log_dw_bid_last(context).as("log_dw_bid_last"),
      col("log_dw_bid_deal"),
      col("log_dw_bid_curator"),
      col("log_dw_view"),
      col("video_slot")
    )

  def log_dw_bid(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
          col("log_dw_bid")
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
              col("log_dw_bid_last")
            ).otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
          )
          .otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
            col("log_dw_bid_last")
          ).otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
        )
        .otherwise(
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
                    Array(StructField("graph_provider_member_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("cost_cpm_usd", DoubleType, true)
                    )
                  ),
                  true
                ),
                StructField("revenue_event_type_id", IntegerType, true),
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
                ),
                StructField("insertion_order_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("campaign_group_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("cold_start_price_type", IntegerType, true),
                StructField("discovery_state",       IntegerType, true),
                StructField(
                  "revenue_info",
                  StructType(
                    Array(
                      StructField("total_partner_fees_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("booked_revenue_dollars",  DoubleType, true),
                      StructField("booked_revenue_adv_curr", DoubleType, true),
                      StructField("total_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_profit_microcents", LongType, true),
                      StructField("total_segment_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_feature_costs_microcents",
                                  LongType,
                                  true
                      )
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
                    StructType(
                      Array(StructField("segment_id", IntegerType, true))
                    ),
                    true
                  ),
                  true
                ),
                StructField("predicted_kpi_event_rate",        DoubleType, true),
                StructField("has_crossdevice_reach_extension", ByteType,   true),
                StructField("advertiser_expected_value_ecpm_ac",
                            DoubleType,
                            true
                ),
                StructField("bpp_multiplier",           DoubleType, true),
                StructField("bpp_offset",               DoubleType, true),
                StructField("bid_modifier",             DoubleType, true),
                StructField("payment_value_microcents", LongType,   true),
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
                        StructField("kpi_event_id",    IntegerType, true),
                        StructField("ev_kpi_event_ac", DoubleType,  true),
                        StructField("p_kpi_event",     DoubleType,  true),
                        StructField("bpo_aggressiveness_factor",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_margin_pct", DoubleType, true),
                        StructField("max_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("cold_start_price_ac", DoubleType, true),
                        StructField("dynamic_bid_max_revenue_ac",
                                    DoubleType,
                                    true
                        ),
                        StructField("p_revenue_event",        DoubleType, true),
                        StructField("total_fees_deducted_ac", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("line_item_currency",      StringType,  true),
                StructField("measurement_fee_cpm_usd", DoubleType,  true),
                StructField("measurement_provider_id", IntegerType, true),
                StructField("measurement_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_cost_usd_cpm",
                            DoubleType,
                            true
                ),
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
                              Array(
                                StructField("segment_id",    IntegerType, true),
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
                      Array(
                        StructField("provider_member_id", IntegerType, true),
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
        )
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
            col("log_dw_bid")
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
                col("log_dw_bid_last")
              ).otherwise(
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
                      StructField("revenue_auction_event_type",
                                  IntegerType,
                                  true
                      ),
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
                      StructField("impression_transaction_type",
                                  IntegerType,
                                  true
                      ),
                      StructField("is_deferred", IntegerType, true),
                      StructField("log_type",    IntegerType, true),
                      StructField(
                        "crossdevice_group_anon",
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
                          Array(StructField("graph_provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_cpm_usd", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      StructField("revenue_event_type_id", IntegerType, true),
                      StructField(
                        "targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("insertion_order_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("campaign_group_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("cold_start_price_type", IntegerType, true),
                      StructField("discovery_state",       IntegerType, true),
                      StructField(
                        "revenue_info",
                        StructType(
                          Array(
                            StructField("total_partner_fees_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("booked_revenue_dollars",
                                        DoubleType,
                                        true
                            ),
                            StructField("booked_revenue_adv_curr",
                                        DoubleType,
                                        true
                            ),
                            StructField("total_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_profit_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_segment_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_feature_costs_microcents",
                                        LongType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      StructField("use_revenue_info",   ByteType,   true),
                      StructField("sales_tax_rate_pct", DoubleType, true),
                      StructField("targeted_crossdevice_graph_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("discovery_prediction", DoubleType,  true),
                      StructField("bidding_host_id",      IntegerType, true),
                      StructField("split_id",             IntegerType, true),
                      StructField(
                        "excluded_targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(StructField("segment_id", IntegerType, true))
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("predicted_kpi_event_rate", DoubleType, true),
                      StructField("has_crossdevice_reach_extension",
                                  ByteType,
                                  true
                      ),
                      StructField("advertiser_expected_value_ecpm_ac",
                                  DoubleType,
                                  true
                      ),
                      StructField("bpp_multiplier",           DoubleType, true),
                      StructField("bpp_offset",               DoubleType, true),
                      StructField("bid_modifier",             DoubleType, true),
                      StructField("payment_value_microcents", LongType,   true),
                      StructField(
                        "crossdevice_graph_membership",
                        ArrayType(
                          StructType(
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
                              StructField("kpi_event_id",    IntegerType, true),
                              StructField("ev_kpi_event_ac", DoubleType,  true),
                              StructField("p_kpi_event",     DoubleType,  true),
                              StructField("bpo_aggressiveness_factor",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_margin_pct", DoubleType, true),
                              StructField("max_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("cold_start_price_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("dynamic_bid_max_revenue_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("p_revenue_event", DoubleType, true),
                              StructField("total_fees_deducted_ac",
                                          DoubleType,
                                          true
                              )
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("line_item_currency",      StringType,  true),
                      StructField("measurement_fee_cpm_usd", DoubleType,  true),
                      StructField("measurement_provider_id", IntegerType, true),
                      StructField("measurement_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_cost_usd_cpm",
                                  DoubleType,
                                  true
                      ),
                      StructField(
                        "targeted_segment_details_by_id_type",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("identity_type", IntegerType, true),
                              StructField(
                                "targeted_segment_details",
                                ArrayType(StructType(
                                            Array(StructField("segment_id",
                                                              IntegerType,
                                                              true
                                                  ),
                                                  StructField("last_seen_min",
                                                              IntegerType,
                                                              true
                                                  )
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
                            Array(StructField("provider_member_id",
                                              IntegerType,
                                              true
                                  ),
                                  StructField("cost_usd_cpm", DoubleType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("frequency_cap_type_internal",
                                  IntegerType,
                                  true
                      ),
                      StructField(
                        "modeled_cap_did_override_line_item_daily_cap",
                        ByteType,
                        true
                      ),
                      StructField("modeled_cap_user_sample_rate",
                                  DoubleType,
                                  true
                      ),
                      StructField("bid_rate", DoubleType, true),
                      StructField("district_postal_code_lists",
                                  ArrayType(IntegerType, true),
                                  true
                      ),
                      StructField("pre_bpp_price",        DoubleType,  true),
                      StructField("feature_tests_bitmap", IntegerType, true)
                    )
                  )
                )
              )
            )
            .otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
              col("log_dw_bid_last")
            ).otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
          )
          .otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
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
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
      )
      .otherwise(
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
                  Array(
                    StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
                  )
                ),
                true
              ),
              StructField("revenue_event_type_id", IntegerType, true),
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
              ),
              StructField("insertion_order_budget_interval_id",
                          IntegerType,
                          true
              ),
              StructField("campaign_group_budget_interval_id",
                          IntegerType,
                          true
              ),
              StructField("cold_start_price_type", IntegerType, true),
              StructField("discovery_state",       IntegerType, true),
              StructField(
                "revenue_info",
                StructType(
                  Array(
                    StructField("total_partner_fees_microcents",
                                LongType,
                                true
                    ),
                    StructField("booked_revenue_dollars",      DoubleType, true),
                    StructField("booked_revenue_adv_curr",     DoubleType, true),
                    StructField("total_data_costs_microcents", LongType,   true),
                    StructField("total_profit_microcents",     LongType,   true),
                    StructField("total_segment_data_costs_microcents",
                                LongType,
                                true
                    ),
                    StructField("total_feature_costs_microcents",
                                LongType,
                                true
                    )
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
                ArrayType(StructType(
                            Array(StructField("segment_id", IntegerType, true))
                          ),
                          true
                ),
                true
              ),
              StructField("predicted_kpi_event_rate",        DoubleType, true),
              StructField("has_crossdevice_reach_extension", ByteType,   true),
              StructField("advertiser_expected_value_ecpm_ac",
                          DoubleType,
                          true
              ),
              StructField("bpp_multiplier",           DoubleType, true),
              StructField("bpp_offset",               DoubleType, true),
              StructField("bid_modifier",             DoubleType, true),
              StructField("payment_value_microcents", LongType,   true),
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
                      StructField("kpi_event_id",    IntegerType, true),
                      StructField("ev_kpi_event_ac", DoubleType,  true),
                      StructField("p_kpi_event",     DoubleType,  true),
                      StructField("bpo_aggressiveness_factor",
                                  DoubleType,
                                  true
                      ),
                      StructField("min_margin_pct",           DoubleType, true),
                      StructField("max_revenue_or_bid_value", DoubleType, true),
                      StructField("min_revenue_or_bid_value", DoubleType, true),
                      StructField("cold_start_price_ac",      DoubleType, true),
                      StructField("dynamic_bid_max_revenue_ac",
                                  DoubleType,
                                  true
                      ),
                      StructField("p_revenue_event",        DoubleType, true),
                      StructField("total_fees_deducted_ac", DoubleType, true)
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
                            Array(
                              StructField("segment_id",    IntegerType, true),
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
      )
  }

  def campaign_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
          col("log_dw_bid").getField("campaign_id")
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("campaign_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
            col("log_dw_bid_last").getField("campaign_id")
          ).otherwise(lit(null).cast(IntegerType))
        )
        .otherwise(lit(null).cast(IntegerType))
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
            col("log_dw_bid").getField("campaign_id")
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
                col("log_dw_bid_last").getField("campaign_id")
              ).otherwise(lit(null).cast(IntegerType))
            )
            .otherwise(lit(null).cast(IntegerType))
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("campaign_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      )
      .otherwise(lit(null).cast(IntegerType))
  }

  def insertion_order_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
          col("log_dw_bid").getField("insertion_order_id")
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("insertion_order_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
            col("log_dw_bid_last").getField("insertion_order_id")
          ).otherwise(lit(null).cast(IntegerType))
        )
        .otherwise(lit(null).cast(IntegerType))
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
            col("log_dw_bid").getField("insertion_order_id")
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
                col("log_dw_bid_last").getField("insertion_order_id")
              ).otherwise(lit(null).cast(IntegerType))
            )
            .otherwise(lit(null).cast(IntegerType))
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("insertion_order_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      )
      .otherwise(lit(null).cast(IntegerType))
  }

  def campaign_group_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
          col("log_dw_bid").getField("campaign_group_id")
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("campaign_group_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
            col("log_dw_bid_last").getField("campaign_group_id")
          ).otherwise(lit(null).cast(IntegerType))
        )
        .otherwise(lit(null).cast(IntegerType))
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
            col("log_dw_bid").getField("campaign_group_id")
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
                col("log_dw_bid_last").getField("campaign_group_id")
              ).otherwise(lit(null).cast(IntegerType))
            )
            .otherwise(lit(null).cast(IntegerType))
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("campaign_group_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      )
      .otherwise(lit(null).cast(IntegerType))
  }

  def advertiser_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
          col("log_dw_bid").getField("advertiser_id")
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("advertiser_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
            col("log_dw_bid_last").getField("advertiser_id")
          ).otherwise(lit(null).cast(IntegerType))
        )
        .otherwise(lit(null).cast(IntegerType))
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
            col("log_dw_bid").getField("advertiser_id")
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
                col("log_dw_bid_last").getField("advertiser_id")
              ).otherwise(lit(null).cast(IntegerType))
            )
            .otherwise(lit(null).cast(IntegerType))
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
              col("log_dw_bid_last").getField("advertiser_id")
            ).otherwise(lit(null).cast(IntegerType))
          )
          .otherwise(lit(null).cast(IntegerType))
      )
      .otherwise(lit(null).cast(IntegerType))
  }

  def log_dw_bid_last(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt"))
        .and(
          is_not_null(
            col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
          )
        )
        .and(
          is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType))
        ),
      when(
        is_not_null(col("log_dw_bid"))
          .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
          .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
        when(
          (col("log_impbus_preempt.buyer_member_id").cast(IntegerType) === col(
            "log_dw_bid.member_id"
          ).cast(IntegerType)).and(
            col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
              "log_dw_bid.creative_id"
            ).cast(IntegerType)
          ),
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
                    Array(StructField("graph_provider_member_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("cost_cpm_usd", DoubleType, true)
                    )
                  ),
                  true
                ),
                StructField("revenue_event_type_id", IntegerType, true),
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
                ),
                StructField("insertion_order_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("campaign_group_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("cold_start_price_type", IntegerType, true),
                StructField("discovery_state",       IntegerType, true),
                StructField(
                  "revenue_info",
                  StructType(
                    Array(
                      StructField("total_partner_fees_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("booked_revenue_dollars",  DoubleType, true),
                      StructField("booked_revenue_adv_curr", DoubleType, true),
                      StructField("total_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_profit_microcents", LongType, true),
                      StructField("total_segment_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_feature_costs_microcents",
                                  LongType,
                                  true
                      )
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
                    StructType(
                      Array(StructField("segment_id", IntegerType, true))
                    ),
                    true
                  ),
                  true
                ),
                StructField("predicted_kpi_event_rate",        DoubleType, true),
                StructField("has_crossdevice_reach_extension", ByteType,   true),
                StructField("advertiser_expected_value_ecpm_ac",
                            DoubleType,
                            true
                ),
                StructField("bpp_multiplier",           DoubleType, true),
                StructField("bpp_offset",               DoubleType, true),
                StructField("bid_modifier",             DoubleType, true),
                StructField("payment_value_microcents", LongType,   true),
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
                        StructField("kpi_event_id",    IntegerType, true),
                        StructField("ev_kpi_event_ac", DoubleType,  true),
                        StructField("p_kpi_event",     DoubleType,  true),
                        StructField("bpo_aggressiveness_factor",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_margin_pct", DoubleType, true),
                        StructField("max_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("cold_start_price_ac", DoubleType, true),
                        StructField("dynamic_bid_max_revenue_ac",
                                    DoubleType,
                                    true
                        ),
                        StructField("p_revenue_event",        DoubleType, true),
                        StructField("total_fees_deducted_ac", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("line_item_currency",      StringType,  true),
                StructField("measurement_fee_cpm_usd", DoubleType,  true),
                StructField("measurement_provider_id", IntegerType, true),
                StructField("measurement_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_cost_usd_cpm",
                            DoubleType,
                            true
                ),
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
                              Array(
                                StructField("segment_id",    IntegerType, true),
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
                      Array(
                        StructField("provider_member_id", IntegerType, true),
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
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_preempt.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                  "log_dw_bid_last.creative_id"
                ).cast(IntegerType)
              ),
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            ).otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
          )
          .otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
      ).when(
          is_not_null(col("log_dw_bid_last"))
            .and(
              is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
            )
            .and(
              is_not_null(col("log_dw_bid_last.creative_id").cast(IntegerType))
            ),
          when(
            (col("log_impbus_preempt.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
              col("log_impbus_preempt.creative_id").cast(IntegerType) === col(
                "log_dw_bid_last.creative_id"
              ).cast(IntegerType)
            ),
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          ).otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
        )
        .otherwise(
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
                    Array(StructField("graph_provider_member_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("cost_cpm_usd", DoubleType, true)
                    )
                  ),
                  true
                ),
                StructField("revenue_event_type_id", IntegerType, true),
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
                ),
                StructField("insertion_order_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("campaign_group_budget_interval_id",
                            IntegerType,
                            true
                ),
                StructField("cold_start_price_type", IntegerType, true),
                StructField("discovery_state",       IntegerType, true),
                StructField(
                  "revenue_info",
                  StructType(
                    Array(
                      StructField("total_partner_fees_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("booked_revenue_dollars",  DoubleType, true),
                      StructField("booked_revenue_adv_curr", DoubleType, true),
                      StructField("total_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_profit_microcents", LongType, true),
                      StructField("total_segment_data_costs_microcents",
                                  LongType,
                                  true
                      ),
                      StructField("total_feature_costs_microcents",
                                  LongType,
                                  true
                      )
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
                    StructType(
                      Array(StructField("segment_id", IntegerType, true))
                    ),
                    true
                  ),
                  true
                ),
                StructField("predicted_kpi_event_rate",        DoubleType, true),
                StructField("has_crossdevice_reach_extension", ByteType,   true),
                StructField("advertiser_expected_value_ecpm_ac",
                            DoubleType,
                            true
                ),
                StructField("bpp_multiplier",           DoubleType, true),
                StructField("bpp_offset",               DoubleType, true),
                StructField("bid_modifier",             DoubleType, true),
                StructField("payment_value_microcents", LongType,   true),
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
                        StructField("kpi_event_id",    IntegerType, true),
                        StructField("ev_kpi_event_ac", DoubleType,  true),
                        StructField("p_kpi_event",     DoubleType,  true),
                        StructField("bpo_aggressiveness_factor",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_margin_pct", DoubleType, true),
                        StructField("max_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("min_revenue_or_bid_value",
                                    DoubleType,
                                    true
                        ),
                        StructField("cold_start_price_ac", DoubleType, true),
                        StructField("dynamic_bid_max_revenue_ac",
                                    DoubleType,
                                    true
                        ),
                        StructField("p_revenue_event",        DoubleType, true),
                        StructField("total_fees_deducted_ac", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("line_item_currency",      StringType,  true),
                StructField("measurement_fee_cpm_usd", DoubleType,  true),
                StructField("measurement_provider_id", IntegerType, true),
                StructField("measurement_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_provider_member_id",
                            IntegerType,
                            true
                ),
                StructField("offline_attribution_cost_usd_cpm",
                            DoubleType,
                            true
                ),
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
                              Array(
                                StructField("segment_id",    IntegerType, true),
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
                      Array(
                        StructField("provider_member_id", IntegerType, true),
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
        )
    ).when(
        is_not_null(col("log_impbus_impressions"))
          .and(
            is_not_null(
              col("log_impbus_impressions.is_delivered").cast(IntegerType)
            )
          )
          .and(
            col("log_impbus_impressions.is_delivered")
              .cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.buyer_member_id").cast(IntegerType)
            )
          )
          .and(
            is_not_null(
              col("log_impbus_impressions.creative_id").cast(IntegerType)
            )
          ),
        when(
          is_not_null(col("log_dw_bid"))
            .and(is_not_null(col("log_dw_bid.member_id").cast(IntegerType)))
            .and(is_not_null(col("log_dw_bid.creative_id").cast(IntegerType))),
          when(
            (col("log_impbus_impressions.buyer_member_id").cast(
              IntegerType
            ) === col("log_dw_bid.member_id").cast(IntegerType)).and(
              col("log_impbus_impressions.creative_id").cast(
                IntegerType
              ) === col("log_dw_bid.creative_id").cast(IntegerType)
            ),
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
                  StructField(
                    "targeted_segment_details_by_id_type",
                    ArrayType(
                      StructType(
                        Array(
                          StructField("identity_type", IntegerType, true),
                          StructField(
                            "targeted_segment_details",
                            ArrayType(StructType(
                                        Array(StructField("segment_id",
                                                          IntegerType,
                                                          true
                                              ),
                                              StructField("last_seen_min",
                                                          IntegerType,
                                                          true
                                              )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          ).when(
              is_not_null(col("log_dw_bid_last"))
                .and(
                  is_not_null(
                    col("log_dw_bid_last.member_id").cast(IntegerType)
                  )
                )
                .and(
                  is_not_null(
                    col("log_dw_bid_last.creative_id").cast(IntegerType)
                  )
                ),
              when(
                (col("log_impbus_impressions.buyer_member_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                  col("log_impbus_impressions.creative_id").cast(
                    IntegerType
                  ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
                ),
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
                      StructField("revenue_auction_event_type",
                                  IntegerType,
                                  true
                      ),
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
                      StructField("impression_transaction_type",
                                  IntegerType,
                                  true
                      ),
                      StructField("is_deferred", IntegerType, true),
                      StructField("log_type",    IntegerType, true),
                      StructField(
                        "crossdevice_group_anon",
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
                          Array(StructField("graph_provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_cpm_usd", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      StructField("revenue_event_type_id", IntegerType, true),
                      StructField(
                        "targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("insertion_order_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("campaign_group_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("cold_start_price_type", IntegerType, true),
                      StructField("discovery_state",       IntegerType, true),
                      StructField(
                        "revenue_info",
                        StructType(
                          Array(
                            StructField("total_partner_fees_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("booked_revenue_dollars",
                                        DoubleType,
                                        true
                            ),
                            StructField("booked_revenue_adv_curr",
                                        DoubleType,
                                        true
                            ),
                            StructField("total_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_profit_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_segment_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_feature_costs_microcents",
                                        LongType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      StructField("use_revenue_info",   ByteType,   true),
                      StructField("sales_tax_rate_pct", DoubleType, true),
                      StructField("targeted_crossdevice_graph_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("discovery_prediction", DoubleType,  true),
                      StructField("bidding_host_id",      IntegerType, true),
                      StructField("split_id",             IntegerType, true),
                      StructField(
                        "excluded_targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(StructField("segment_id", IntegerType, true))
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("predicted_kpi_event_rate", DoubleType, true),
                      StructField("has_crossdevice_reach_extension",
                                  ByteType,
                                  true
                      ),
                      StructField("advertiser_expected_value_ecpm_ac",
                                  DoubleType,
                                  true
                      ),
                      StructField("bpp_multiplier",           DoubleType, true),
                      StructField("bpp_offset",               DoubleType, true),
                      StructField("bid_modifier",             DoubleType, true),
                      StructField("payment_value_microcents", LongType,   true),
                      StructField(
                        "crossdevice_graph_membership",
                        ArrayType(
                          StructType(
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
                              StructField("kpi_event_id",    IntegerType, true),
                              StructField("ev_kpi_event_ac", DoubleType,  true),
                              StructField("p_kpi_event",     DoubleType,  true),
                              StructField("bpo_aggressiveness_factor",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_margin_pct", DoubleType, true),
                              StructField("max_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("cold_start_price_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("dynamic_bid_max_revenue_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("p_revenue_event", DoubleType, true),
                              StructField("total_fees_deducted_ac",
                                          DoubleType,
                                          true
                              )
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("line_item_currency",      StringType,  true),
                      StructField("measurement_fee_cpm_usd", DoubleType,  true),
                      StructField("measurement_provider_id", IntegerType, true),
                      StructField("measurement_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_cost_usd_cpm",
                                  DoubleType,
                                  true
                      ),
                      StructField(
                        "targeted_segment_details_by_id_type",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("identity_type", IntegerType, true),
                              StructField(
                                "targeted_segment_details",
                                ArrayType(StructType(
                                            Array(StructField("segment_id",
                                                              IntegerType,
                                                              true
                                                  ),
                                                  StructField("last_seen_min",
                                                              IntegerType,
                                                              true
                                                  )
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
                            Array(StructField("provider_member_id",
                                              IntegerType,
                                              true
                                  ),
                                  StructField("cost_usd_cpm", DoubleType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("frequency_cap_type_internal",
                                  IntegerType,
                                  true
                      ),
                      StructField(
                        "modeled_cap_did_override_line_item_daily_cap",
                        ByteType,
                        true
                      ),
                      StructField("modeled_cap_user_sample_rate",
                                  DoubleType,
                                  true
                      ),
                      StructField("bid_rate", DoubleType, true),
                      StructField("district_postal_code_lists",
                                  ArrayType(IntegerType, true),
                                  true
                      ),
                      StructField("pre_bpp_price",        DoubleType,  true),
                      StructField("feature_tests_bitmap", IntegerType, true)
                    )
                  )
                )
              ).otherwise(
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
                      StructField("revenue_auction_event_type",
                                  IntegerType,
                                  true
                      ),
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
                      StructField("impression_transaction_type",
                                  IntegerType,
                                  true
                      ),
                      StructField("is_deferred", IntegerType, true),
                      StructField("log_type",    IntegerType, true),
                      StructField(
                        "crossdevice_group_anon",
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
                          Array(StructField("graph_provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_cpm_usd", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      StructField("revenue_event_type_id", IntegerType, true),
                      StructField(
                        "targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("segment_id",    IntegerType, true),
                              StructField("last_seen_min", IntegerType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("insertion_order_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("campaign_group_budget_interval_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("cold_start_price_type", IntegerType, true),
                      StructField("discovery_state",       IntegerType, true),
                      StructField(
                        "revenue_info",
                        StructType(
                          Array(
                            StructField("total_partner_fees_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("booked_revenue_dollars",
                                        DoubleType,
                                        true
                            ),
                            StructField("booked_revenue_adv_curr",
                                        DoubleType,
                                        true
                            ),
                            StructField("total_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_profit_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_segment_data_costs_microcents",
                                        LongType,
                                        true
                            ),
                            StructField("total_feature_costs_microcents",
                                        LongType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      StructField("use_revenue_info",   ByteType,   true),
                      StructField("sales_tax_rate_pct", DoubleType, true),
                      StructField("targeted_crossdevice_graph_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("product_feed_id", IntegerType, true),
                      StructField("item_selection_strategy_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("discovery_prediction", DoubleType,  true),
                      StructField("bidding_host_id",      IntegerType, true),
                      StructField("split_id",             IntegerType, true),
                      StructField(
                        "excluded_targeted_segment_details",
                        ArrayType(
                          StructType(
                            Array(StructField("segment_id", IntegerType, true))
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("predicted_kpi_event_rate", DoubleType, true),
                      StructField("has_crossdevice_reach_extension",
                                  ByteType,
                                  true
                      ),
                      StructField("advertiser_expected_value_ecpm_ac",
                                  DoubleType,
                                  true
                      ),
                      StructField("bpp_multiplier",           DoubleType, true),
                      StructField("bpp_offset",               DoubleType, true),
                      StructField("bid_modifier",             DoubleType, true),
                      StructField("payment_value_microcents", LongType,   true),
                      StructField(
                        "crossdevice_graph_membership",
                        ArrayType(
                          StructType(
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
                              StructField("kpi_event_id",    IntegerType, true),
                              StructField("ev_kpi_event_ac", DoubleType,  true),
                              StructField("p_kpi_event",     DoubleType,  true),
                              StructField("bpo_aggressiveness_factor",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_margin_pct", DoubleType, true),
                              StructField("max_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("min_revenue_or_bid_value",
                                          DoubleType,
                                          true
                              ),
                              StructField("cold_start_price_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("dynamic_bid_max_revenue_ac",
                                          DoubleType,
                                          true
                              ),
                              StructField("p_revenue_event", DoubleType, true),
                              StructField("total_fees_deducted_ac",
                                          DoubleType,
                                          true
                              )
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("line_item_currency",      StringType,  true),
                      StructField("measurement_fee_cpm_usd", DoubleType,  true),
                      StructField("measurement_provider_id", IntegerType, true),
                      StructField("measurement_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_provider_member_id",
                                  IntegerType,
                                  true
                      ),
                      StructField("offline_attribution_cost_usd_cpm",
                                  DoubleType,
                                  true
                      ),
                      StructField(
                        "targeted_segment_details_by_id_type",
                        ArrayType(
                          StructType(
                            Array(
                              StructField("identity_type", IntegerType, true),
                              StructField(
                                "targeted_segment_details",
                                ArrayType(StructType(
                                            Array(StructField("segment_id",
                                                              IntegerType,
                                                              true
                                                  ),
                                                  StructField("last_seen_min",
                                                              IntegerType,
                                                              true
                                                  )
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
                            Array(StructField("provider_member_id",
                                              IntegerType,
                                              true
                                  ),
                                  StructField("cost_usd_cpm", DoubleType, true)
                            )
                          ),
                          true
                        ),
                        true
                      ),
                      StructField("frequency_cap_type_internal",
                                  IntegerType,
                                  true
                      ),
                      StructField(
                        "modeled_cap_did_override_line_item_daily_cap",
                        ByteType,
                        true
                      ),
                      StructField("modeled_cap_user_sample_rate",
                                  DoubleType,
                                  true
                      ),
                      StructField("bid_rate", DoubleType, true),
                      StructField("district_postal_code_lists",
                                  ArrayType(IntegerType, true),
                                  true
                      ),
                      StructField("pre_bpp_price",        DoubleType,  true),
                      StructField("feature_tests_bitmap", IntegerType, true)
                    )
                  )
                )
              )
            )
            .otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
        ).when(
            is_not_null(col("log_dw_bid_last"))
              .and(
                is_not_null(col("log_dw_bid_last.member_id").cast(IntegerType))
              )
              .and(
                is_not_null(
                  col("log_dw_bid_last.creative_id").cast(IntegerType)
                )
              ),
            when(
              (col("log_impbus_impressions.buyer_member_id").cast(
                IntegerType
              ) === col("log_dw_bid_last.member_id").cast(IntegerType)).and(
                col("log_impbus_impressions.creative_id").cast(
                  IntegerType
                ) === col("log_dw_bid_last.creative_id").cast(IntegerType)
              ),
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            ).otherwise(
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
                    StructField("revenue_auction_event_type",
                                IntegerType,
                                true
                    ),
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
                    StructField("impression_transaction_type",
                                IntegerType,
                                true
                    ),
                    StructField("is_deferred", IntegerType, true),
                    StructField("log_type",    IntegerType, true),
                    StructField(
                      "crossdevice_group_anon",
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
                        Array(StructField("graph_provider_member_id",
                                          IntegerType,
                                          true
                              ),
                              StructField("cost_cpm_usd", DoubleType, true)
                        )
                      ),
                      true
                    ),
                    StructField("revenue_event_type_id", IntegerType, true),
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
                    ),
                    StructField("insertion_order_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("campaign_group_budget_interval_id",
                                IntegerType,
                                true
                    ),
                    StructField("cold_start_price_type", IntegerType, true),
                    StructField("discovery_state",       IntegerType, true),
                    StructField(
                      "revenue_info",
                      StructType(
                        Array(
                          StructField("total_partner_fees_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("booked_revenue_dollars",
                                      DoubleType,
                                      true
                          ),
                          StructField("booked_revenue_adv_curr",
                                      DoubleType,
                                      true
                          ),
                          StructField("total_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_profit_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_segment_data_costs_microcents",
                                      LongType,
                                      true
                          ),
                          StructField("total_feature_costs_microcents",
                                      LongType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    StructField("use_revenue_info",   ByteType,   true),
                    StructField("sales_tax_rate_pct", DoubleType, true),
                    StructField("targeted_crossdevice_graph_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("discovery_prediction", DoubleType,  true),
                    StructField("bidding_host_id",      IntegerType, true),
                    StructField("split_id",             IntegerType, true),
                    StructField(
                      "excluded_targeted_segment_details",
                      ArrayType(
                        StructType(
                          Array(StructField("segment_id", IntegerType, true))
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("predicted_kpi_event_rate", DoubleType, true),
                    StructField("has_crossdevice_reach_extension",
                                ByteType,
                                true
                    ),
                    StructField("advertiser_expected_value_ecpm_ac",
                                DoubleType,
                                true
                    ),
                    StructField("bpp_multiplier",           DoubleType, true),
                    StructField("bpp_offset",               DoubleType, true),
                    StructField("bid_modifier",             DoubleType, true),
                    StructField("payment_value_microcents", LongType,   true),
                    StructField(
                      "crossdevice_graph_membership",
                      ArrayType(
                        StructType(
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
                            StructField("kpi_event_id",    IntegerType, true),
                            StructField("ev_kpi_event_ac", DoubleType,  true),
                            StructField("p_kpi_event",     DoubleType,  true),
                            StructField("bpo_aggressiveness_factor",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_margin_pct", DoubleType, true),
                            StructField("max_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("min_revenue_or_bid_value",
                                        DoubleType,
                                        true
                            ),
                            StructField("cold_start_price_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("dynamic_bid_max_revenue_ac",
                                        DoubleType,
                                        true
                            ),
                            StructField("p_revenue_event", DoubleType, true),
                            StructField("total_fees_deducted_ac",
                                        DoubleType,
                                        true
                            )
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("line_item_currency",      StringType,  true),
                    StructField("measurement_fee_cpm_usd", DoubleType,  true),
                    StructField("measurement_provider_id", IntegerType, true),
                    StructField("measurement_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_provider_member_id",
                                IntegerType,
                                true
                    ),
                    StructField("offline_attribution_cost_usd_cpm",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "targeted_segment_details_by_id_type",
                      ArrayType(
                        StructType(
                          Array(
                            StructField("identity_type", IntegerType, true),
                            StructField(
                              "targeted_segment_details",
                              ArrayType(StructType(
                                          Array(StructField("segment_id",
                                                            IntegerType,
                                                            true
                                                ),
                                                StructField("last_seen_min",
                                                            IntegerType,
                                                            true
                                                )
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
                          Array(StructField("provider_member_id",
                                            IntegerType,
                                            true
                                ),
                                StructField("cost_usd_cpm", DoubleType, true)
                          )
                        ),
                        true
                      ),
                      true
                    ),
                    StructField("frequency_cap_type_internal",
                                IntegerType,
                                true
                    ),
                    StructField("modeled_cap_did_override_line_item_daily_cap",
                                ByteType,
                                true
                    ),
                    StructField("modeled_cap_user_sample_rate",
                                DoubleType,
                                true
                    ),
                    StructField("bid_rate", DoubleType, true),
                    StructField("district_postal_code_lists",
                                ArrayType(IntegerType, true),
                                true
                    ),
                    StructField("pre_bpp_price",        DoubleType,  true),
                    StructField("feature_tests_bitmap", IntegerType, true)
                  )
                )
              )
            )
          )
          .otherwise(
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
                  StructField(
                    "crossdevice_group_anon",
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
                      Array(StructField("graph_provider_member_id",
                                        IntegerType,
                                        true
                            ),
                            StructField("cost_cpm_usd", DoubleType, true)
                      )
                    ),
                    true
                  ),
                  StructField("revenue_event_type_id", IntegerType, true),
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
                  ),
                  StructField("insertion_order_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("campaign_group_budget_interval_id",
                              IntegerType,
                              true
                  ),
                  StructField("cold_start_price_type", IntegerType, true),
                  StructField("discovery_state",       IntegerType, true),
                  StructField(
                    "revenue_info",
                    StructType(
                      Array(
                        StructField("total_partner_fees_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("booked_revenue_dollars", DoubleType, true),
                        StructField("booked_revenue_adv_curr",
                                    DoubleType,
                                    true
                        ),
                        StructField("total_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_profit_microcents", LongType, true),
                        StructField("total_segment_data_costs_microcents",
                                    LongType,
                                    true
                        ),
                        StructField("total_feature_costs_microcents",
                                    LongType,
                                    true
                        )
                      )
                    ),
                    true
                  ),
                  StructField("use_revenue_info",   ByteType,   true),
                  StructField("sales_tax_rate_pct", DoubleType, true),
                  StructField("targeted_crossdevice_graph_id",
                              IntegerType,
                              true
                  ),
                  StructField("product_feed_id",            IntegerType, true),
                  StructField("item_selection_strategy_id", IntegerType, true),
                  StructField("discovery_prediction",       DoubleType,  true),
                  StructField("bidding_host_id",            IntegerType, true),
                  StructField("split_id",                   IntegerType, true),
                  StructField(
                    "excluded_targeted_segment_details",
                    ArrayType(
                      StructType(
                        Array(StructField("segment_id", IntegerType, true))
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("predicted_kpi_event_rate", DoubleType, true),
                  StructField("has_crossdevice_reach_extension",
                              ByteType,
                              true
                  ),
                  StructField("advertiser_expected_value_ecpm_ac",
                              DoubleType,
                              true
                  ),
                  StructField("bpp_multiplier",           DoubleType, true),
                  StructField("bpp_offset",               DoubleType, true),
                  StructField("bid_modifier",             DoubleType, true),
                  StructField("payment_value_microcents", LongType,   true),
                  StructField(
                    "crossdevice_graph_membership",
                    ArrayType(
                      StructType(
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
                          StructField("kpi_event_id",    IntegerType, true),
                          StructField("ev_kpi_event_ac", DoubleType,  true),
                          StructField("p_kpi_event",     DoubleType,  true),
                          StructField("bpo_aggressiveness_factor",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_margin_pct", DoubleType, true),
                          StructField("max_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("min_revenue_or_bid_value",
                                      DoubleType,
                                      true
                          ),
                          StructField("cold_start_price_ac", DoubleType, true),
                          StructField("dynamic_bid_max_revenue_ac",
                                      DoubleType,
                                      true
                          ),
                          StructField("p_revenue_event", DoubleType, true),
                          StructField("total_fees_deducted_ac",
                                      DoubleType,
                                      true
                          )
                        )
                      ),
                      true
                    ),
                    true
                  ),
                  StructField("line_item_currency",      StringType,  true),
                  StructField("measurement_fee_cpm_usd", DoubleType,  true),
                  StructField("measurement_provider_id", IntegerType, true),
                  StructField("measurement_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_provider_member_id",
                              IntegerType,
                              true
                  ),
                  StructField("offline_attribution_cost_usd_cpm",
                              DoubleType,
                              true
                  ),
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
                                Array(
                                  StructField("segment_id", IntegerType, true),
                                  StructField("last_seen_min",
                                              IntegerType,
                                              true
                                  )
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
                        Array(
                          StructField("provider_member_id", IntegerType, true),
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
          )
      )
      .otherwise(
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
                  Array(
                    StructField("graph_provider_member_id", IntegerType, true),
                    StructField("cost_cpm_usd",             DoubleType,  true)
                  )
                ),
                true
              ),
              StructField("revenue_event_type_id", IntegerType, true),
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
              ),
              StructField("insertion_order_budget_interval_id",
                          IntegerType,
                          true
              ),
              StructField("campaign_group_budget_interval_id",
                          IntegerType,
                          true
              ),
              StructField("cold_start_price_type", IntegerType, true),
              StructField("discovery_state",       IntegerType, true),
              StructField(
                "revenue_info",
                StructType(
                  Array(
                    StructField("total_partner_fees_microcents",
                                LongType,
                                true
                    ),
                    StructField("booked_revenue_dollars",      DoubleType, true),
                    StructField("booked_revenue_adv_curr",     DoubleType, true),
                    StructField("total_data_costs_microcents", LongType,   true),
                    StructField("total_profit_microcents",     LongType,   true),
                    StructField("total_segment_data_costs_microcents",
                                LongType,
                                true
                    ),
                    StructField("total_feature_costs_microcents",
                                LongType,
                                true
                    )
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
                ArrayType(StructType(
                            Array(StructField("segment_id", IntegerType, true))
                          ),
                          true
                ),
                true
              ),
              StructField("predicted_kpi_event_rate",        DoubleType, true),
              StructField("has_crossdevice_reach_extension", ByteType,   true),
              StructField("advertiser_expected_value_ecpm_ac",
                          DoubleType,
                          true
              ),
              StructField("bpp_multiplier",           DoubleType, true),
              StructField("bpp_offset",               DoubleType, true),
              StructField("bid_modifier",             DoubleType, true),
              StructField("payment_value_microcents", LongType,   true),
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
                      StructField("kpi_event_id",    IntegerType, true),
                      StructField("ev_kpi_event_ac", DoubleType,  true),
                      StructField("p_kpi_event",     DoubleType,  true),
                      StructField("bpo_aggressiveness_factor",
                                  DoubleType,
                                  true
                      ),
                      StructField("min_margin_pct",           DoubleType, true),
                      StructField("max_revenue_or_bid_value", DoubleType, true),
                      StructField("min_revenue_or_bid_value", DoubleType, true),
                      StructField("cold_start_price_ac",      DoubleType, true),
                      StructField("dynamic_bid_max_revenue_ac",
                                  DoubleType,
                                  true
                      ),
                      StructField("p_revenue_event",        DoubleType, true),
                      StructField("total_fees_deducted_ac", DoubleType, true)
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
                            Array(
                              StructField("segment_id",    IntegerType, true),
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
      )
  }

}
