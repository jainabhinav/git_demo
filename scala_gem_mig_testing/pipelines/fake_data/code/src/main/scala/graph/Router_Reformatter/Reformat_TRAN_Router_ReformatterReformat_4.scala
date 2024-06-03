package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Router_Reformatter.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_TRAN_Router_ReformatterReformat_4 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      bids(context).as("bids"),
      dw_views(context).as("dw_views"),
      auction_events(context).as("auction_events"),
      impressions(context).as("impressions"),
      pricings(context).as("pricings"),
      imps_seen(context).as("imps_seen"),
      preempts(context).as("preempts"),
      lit(null)
        .cast(
          ArrayType(
            StructType(
              Array(
                StructField("date_time",                LongType,                     true),
                StructField("auction_id_64",            LongType,                     true),
                StructField("verifier_member_id",       IntegerType,                  true),
                StructField("verifier_detected_domain", StringType,                   true),
                StructField("reason_ids",               ArrayType(IntegerType, true), true)
              )
            ),
            true
          )
        )
        .as("spend_protections"),
      impbus_views(context).as("impbus_views"),
      stage_seen_out(context).as("stage_seen_out"),
      dw_imps_out(context).as("dw_imps_out"),
      filter(
        array(
          when(coalesce(
                 col("log_impbus_impressions_pricing_count").cast(IntegerType),
                 lit(1)
               ) > lit(1),
               struct(lit(2).as("reason"))
          ),
          when(quarantine_sell_side_hierarchy() === lit(1),
               struct(lit(4).as("reason"))
          ),
          when(quarantine_buy_side_hierarchy() === lit(1),
               struct(lit(3).as("reason"))
          )
        ),
        i => is_not_null(i)
      ).as("quarantine_reasons")
    )

  def pricings(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_impressions_pricing")),
      filter(
        array(
          col("log_impbus_impressions_pricing"),
          when(
            is_not_null(col("log_impbus_impressions_pricing_dup")).and(
              coalesce(
                col("log_impbus_impressions_pricing_count").cast(IntegerType),
                lit(1)
              ).cast(IntegerType) > lit(1)
            ),
            col("log_impbus_impressions_pricing_dup")
          )
        ),
        xx => is_not_null(xx)
      )
    ).otherwise(
      lit(null).cast(
        ArrayType(
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
                    StructField("gross_payment_value_microcents",
                                LongType,
                                true
                    ),
                    StructField("net_payment_value_microcents", LongType, true),
                    StructField("seller_revenue_microcents",    LongType, true),
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
                                  StructField("data_member_id",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          ),
                          StructField("fx_margin_rate_id", IntegerType, true),
                          StructField("marketplace_owner_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("virtual_marketplace_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("amino_enabled", ByteType, true)
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
                                  StructField("data_member_id",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          ),
                          StructField("fx_margin_rate_id", IntegerType, true),
                          StructField("marketplace_owner_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("virtual_marketplace_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("amino_enabled", ByteType, true)
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
          ),
          true
        )
      )
    )
  }

  def impbus_views(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(is_not_null(col("log_impbus_view")), array(col("log_impbus_view")))
      .otherwise(
        lit(null).cast(
          ArrayType(
            StructType(
              Array(
                StructField("date_time",                     LongType,    true),
                StructField("auction_id_64",                 LongType,    true),
                StructField("user_id_64",                    LongType,    true),
                StructField("view_result",                   IntegerType, true),
                StructField("ttl",                           IntegerType, true),
                StructField("view_data",                     StringType,  true),
                StructField("viewdef_definition_id",         IntegerType, true),
                StructField("viewdef_view_result",           IntegerType, true),
                StructField("view_not_measurable_type",      IntegerType, true),
                StructField("view_not_visible_type",         IntegerType, true),
                StructField("view_frame_type",               IntegerType, true),
                StructField("view_script_version",           IntegerType, true),
                StructField("view_tag_version",              StringType,  true),
                StructField("view_screen_width",             IntegerType, true),
                StructField("view_screen_height",            IntegerType, true),
                StructField("view_js_browser",               StringType,  true),
                StructField("view_js_platform",              StringType,  true),
                StructField("view_banner_left",              IntegerType, true),
                StructField("view_banner_top",               IntegerType, true),
                StructField("view_banner_width",             IntegerType, true),
                StructField("view_banner_height",            IntegerType, true),
                StructField("view_tracking_duration",        DoubleType,  true),
                StructField("view_page_duration",            DoubleType,  true),
                StructField("view_usage_duration",           DoubleType,  true),
                StructField("view_surface",                  DoubleType,  true),
                StructField("view_js_message",               StringType,  true),
                StructField("view_player_width",             IntegerType, true),
                StructField("view_player_height",            IntegerType, true),
                StructField("view_iab_duration",             DoubleType,  true),
                StructField("view_iab_inview_count",         IntegerType, true),
                StructField("view_duration_gt_0pct",         DoubleType,  true),
                StructField("view_duration_gt_25pct",        DoubleType,  true),
                StructField("view_duration_gt_50pct",        DoubleType,  true),
                StructField("view_duration_gt_75pct",        DoubleType,  true),
                StructField("view_duration_eq_100pct",       DoubleType,  true),
                StructField("auction_timestamp",             LongType,    true),
                StructField("view_has_banner_left",          IntegerType, true),
                StructField("view_has_banner_top",           IntegerType, true),
                StructField("view_mouse_position_final_x",   IntegerType, true),
                StructField("view_mouse_position_final_y",   IntegerType, true),
                StructField("view_has_mouse_position_final", IntegerType, true),
                StructField("view_mouse_position_initial_x", IntegerType, true),
                StructField("view_mouse_position_initial_y", IntegerType, true),
                StructField("view_has_mouse_position_initial",
                            IntegerType,
                            true
                ),
                StructField("view_mouse_position_page_x",    IntegerType, true),
                StructField("view_mouse_position_page_y",    IntegerType, true),
                StructField("view_has_mouse_position_page",  IntegerType, true),
                StructField("view_mouse_position_timeout_x", IntegerType, true),
                StructField("view_mouse_position_timeout_y", IntegerType, true),
                StructField("view_has_mouse_position_timeout",
                            IntegerType,
                            true
                ),
                StructField("view_session_id", LongType, true),
                StructField(
                  "view_video",
                  StructType(
                    Array(
                      StructField("view_audio_duration_eq_100pct",
                                  DoubleType,
                                  true
                      ),
                      StructField("view_creative_duration", DoubleType, true)
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
            ),
            true
          )
        )
      )
  }

  def preempts(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_preempt")),
      filter(
        array(
          col("log_impbus_preempt"),
          when(
            is_not_null(col("log_impbus_preempt_dup")).and(
              coalesce(
                col("log_impbus_impressions_pricing_count").cast(IntegerType),
                lit(1)
              ).cast(IntegerType) > lit(1)
            ),
            col("log_impbus_preempt_dup")
          )
        ),
        xx => is_not_null(xx)
      )
    ).otherwise(
      lit(null).cast(
        ArrayType(
          StructType(
            Array(
              StructField("date_time",              LongType,    true),
              StructField("auction_id_64",          LongType,    true),
              StructField("imp_transacted",         IntegerType, true),
              StructField("buyer_spend",            DoubleType,  true),
              StructField("seller_revenue",         DoubleType,  true),
              StructField("bidder_fees",            DoubleType,  true),
              StructField("instance_id",            IntegerType, true),
              StructField("fold_position",          IntegerType, true),
              StructField("seller_deduction",       DoubleType,  true),
              StructField("buyer_member_id",        IntegerType, true),
              StructField("creative_id",            IntegerType, true),
              StructField("cleared_direct",         IntegerType, true),
              StructField("buyer_currency",         StringType,  true),
              StructField("buyer_exchange_rate",    DoubleType,  true),
              StructField("width",                  IntegerType, true),
              StructField("height",                 IntegerType, true),
              StructField("brand_id",               IntegerType, true),
              StructField("creative_audit_status",  IntegerType, true),
              StructField("is_creative_hosted",     IntegerType, true),
              StructField("vp_expose_domains",      IntegerType, true),
              StructField("vp_expose_categories",   IntegerType, true),
              StructField("vp_expose_pubs",         IntegerType, true),
              StructField("vp_expose_tag",          IntegerType, true),
              StructField("bidder_id",              IntegerType, true),
              StructField("deal_id",                IntegerType, true),
              StructField("imp_type",               IntegerType, true),
              StructField("is_dw",                  IntegerType, true),
              StructField("vp_bitmap",              LongType,    true),
              StructField("ttl",                    IntegerType, true),
              StructField("view_detection_enabled", IntegerType, true),
              StructField("media_type",             IntegerType, true),
              StructField("auction_timestamp",      LongType,    true),
              StructField("spend_protection",       IntegerType, true),
              StructField("viewdef_definition_id_buyer_member",
                          IntegerType,
                          true
              ),
              StructField("deal_type",                 IntegerType, true),
              StructField("ym_floor_id",               IntegerType, true),
              StructField("ym_bias_id",                IntegerType, true),
              StructField("bid_price_type",            IntegerType, true),
              StructField("spend_protection_pixel_id", IntegerType, true),
              StructField("ip_address",                StringType,  true),
              StructField(
                "buyer_transaction_def",
                StructType(
                  Array(
                    StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
                  )
                ),
                true
              ),
              StructField(
                "seller_transaction_def",
                StructType(
                  Array(
                    StructField("transaction_event",         IntegerType, true),
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
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_uuid", StringType, true)
                  )
                ),
                true
              ),
              StructField("external_bidrequest_id",     LongType,    true),
              StructField("external_bidrequest_imp_id", LongType,    true),
              StructField("creative_media_subtype_id",  IntegerType, true)
            )
          ),
          true
        )
      )
    )
  }

  def auction_events(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(is_not_null(col("log_impbus_auction_event")),
         array(col("log_impbus_auction_event"))
    ).otherwise(
      lit(null).cast(
        ArrayType(
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
                    StructField("gross_payment_value_microcents",
                                LongType,
                                true
                    ),
                    StructField("net_payment_value_microcents", LongType, true),
                    StructField("seller_revenue_microcents",    LongType, true),
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
                                  StructField("data_member_id",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          ),
                          StructField("fx_margin_rate_id", IntegerType, true),
                          StructField("marketplace_owner_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("virtual_marketplace_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("amino_enabled", ByteType, true)
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
                                  StructField("data_member_id",
                                              IntegerType,
                                              true
                                  )
                                )
                              ),
                              true
                            ),
                            true
                          ),
                          StructField("fx_margin_rate_id", IntegerType, true),
                          StructField("marketplace_owner_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("virtual_marketplace_id",
                                      IntegerType,
                                      true
                          ),
                          StructField("amino_enabled", ByteType, true)
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
          ),
          true
        )
      )
    )
  }

  def imps_seen(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      ArrayType(
        StructType(
          Array(
            StructField("date_time",                  LongType,    true),
            StructField("auction_id_64",              LongType,    true),
            StructField("user_id_64",                 LongType,    true),
            StructField("tag_id",                     IntegerType, true),
            StructField("venue_id",                   IntegerType, true),
            StructField("site_domain",                StringType,  true),
            StructField("width",                      IntegerType, true),
            StructField("height",                     IntegerType, true),
            StructField("geo_country",                StringType,  true),
            StructField("bidder_id",                  IntegerType, true),
            StructField("seller_member_id",           IntegerType, true),
            StructField("imp_blacklist_or_fraud",     IntegerType, true),
            StructField("imp_bid_on",                 IntegerType, true),
            StructField("call_type",                  StringType,  true),
            StructField("inventory_source_id",        IntegerType, true),
            StructField("cleared_direct",             IntegerType, true),
            StructField("fold_position",              IntegerType, true),
            StructField("imp_type",                   IntegerType, true),
            StructField("is_dw",                      IntegerType, true),
            StructField("publisher_id",               IntegerType, true),
            StructField("site_id",                    IntegerType, true),
            StructField("datacenter_id",              IntegerType, true),
            StructField("pub_rule_id",                IntegerType, true),
            StructField("media_type",                 IntegerType, true),
            StructField("inventory_url_id",           IntegerType, true),
            StructField("audit_type",                 IntegerType, true),
            StructField("user_group_id",              IntegerType, true),
            StructField("sampling_pct",               DoubleType,  true),
            StructField("visibility_profile_id",      IntegerType, true),
            StructField("instance_id",                IntegerType, true),
            StructField("cookie_age",                 IntegerType, true),
            StructField("ip_address",                 StringType,  true),
            StructField("truncate_ip",                IntegerType, true),
            StructField("application_id",             StringType,  true),
            StructField("supply_type",                IntegerType, true),
            StructField("operating_system",           IntegerType, true),
            StructField("device_type",                IntegerType, true),
            StructField("operating_system_family_id", IntegerType, true),
            StructField(
              "mobile",
              StructType(
                Array(
                  StructField("device_unique_id", StringType,  true),
                  StructField("device_id",        IntegerType, true),
                  StructField("mobile_location",
                              StructType(
                                Array(StructField("lat", DoubleType, true),
                                      StructField("lon", DoubleType, true)
                                )
                              ),
                              true
                  )
                )
              ),
              true
            ),
            StructField("is_imp_rejecter_applied", ByteType, true),
            StructField("imp_rejecter_do_auction", ByteType, true),
            StructField("allowed_media_types",
                        ArrayType(IntegerType, true),
                        true
            ),
            StructField("is_prebid",                  ByteType,    true),
            StructField("gdpr_consent_cookie",        StringType,  true),
            StructField("subject_to_gdpr",            ByteType,    true),
            StructField("sdk_version",                StringType,  true),
            StructField("browser_code_id",            IntegerType, true),
            StructField("is_prebid_server_included",  IntegerType, true),
            StructField("pred_info",                  IntegerType, true),
            StructField("is_amp",                     ByteType,    true),
            StructField("hb_source",                  IntegerType, true),
            StructField("ss_native_assembly_enabled", ByteType,    true),
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
            StructField("uid_source",               IntegerType, true),
            StructField("openrtb_req_subdomain",    StringType,  true),
            StructField("is_private_auction",       ByteType,    true),
            StructField("private_auction_eligible", ByteType,    true),
            StructField("client_request_id",        StringType,  true),
            StructField("chrome_traffic_label",     IntegerType, true)
          )
        ),
        true
      )
    )
  }

  def impressions(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(is_not_null(col("log_impbus_impressions")),
         array(col("log_impbus_impressions"))
    ).otherwise(
      lit(null).cast(
        ArrayType(
          StructType(
            Array(
              StructField("date_time",                   LongType,    true),
              StructField("auction_id_64",               LongType,    true),
              StructField("user_id_64",                  LongType,    true),
              StructField("tag_id",                      IntegerType, true),
              StructField("ip_address",                  StringType,  true),
              StructField("venue_id",                    IntegerType, true),
              StructField("site_domain",                 StringType,  true),
              StructField("width",                       IntegerType, true),
              StructField("height",                      IntegerType, true),
              StructField("geo_country",                 StringType,  true),
              StructField("geo_region",                  StringType,  true),
              StructField("gender",                      StringType,  true),
              StructField("age",                         IntegerType, true),
              StructField("bidder_id",                   IntegerType, true),
              StructField("seller_member_id",            IntegerType, true),
              StructField("buyer_member_id",             IntegerType, true),
              StructField("creative_id",                 IntegerType, true),
              StructField("imp_blacklist_or_fraud",      IntegerType, true),
              StructField("imp_bid_on",                  IntegerType, true),
              StructField("buyer_bid",                   DoubleType,  true),
              StructField("buyer_spend",                 DoubleType,  true),
              StructField("seller_revenue",              DoubleType,  true),
              StructField("num_of_bids",                 IntegerType, true),
              StructField("ecp",                         DoubleType,  true),
              StructField("reserve_price",               DoubleType,  true),
              StructField("inv_code",                    StringType,  true),
              StructField("call_type",                   StringType,  true),
              StructField("inventory_source_id",         IntegerType, true),
              StructField("cookie_age",                  IntegerType, true),
              StructField("brand_id",                    IntegerType, true),
              StructField("cleared_direct",              IntegerType, true),
              StructField("forex_allowance",             DoubleType,  true),
              StructField("fold_position",               IntegerType, true),
              StructField("external_inv_id",             IntegerType, true),
              StructField("imp_type",                    IntegerType, true),
              StructField("is_delivered",                IntegerType, true),
              StructField("is_dw",                       IntegerType, true),
              StructField("publisher_id",                IntegerType, true),
              StructField("site_id",                     IntegerType, true),
              StructField("content_category_id",         IntegerType, true),
              StructField("datacenter_id",               IntegerType, true),
              StructField("eap",                         DoubleType,  true),
              StructField("user_tz_offset",              IntegerType, true),
              StructField("user_group_id",               IntegerType, true),
              StructField("pub_rule_id",                 IntegerType, true),
              StructField("media_type",                  IntegerType, true),
              StructField("operating_system",            IntegerType, true),
              StructField("browser",                     IntegerType, true),
              StructField("language",                    IntegerType, true),
              StructField("application_id",              StringType,  true),
              StructField("user_locale",                 StringType,  true),
              StructField("inventory_url_id",            IntegerType, true),
              StructField("audit_type",                  IntegerType, true),
              StructField("shadow_price",                DoubleType,  true),
              StructField("impbus_id",                   IntegerType, true),
              StructField("buyer_currency",              StringType,  true),
              StructField("buyer_exchange_rate",         DoubleType,  true),
              StructField("seller_currency",             StringType,  true),
              StructField("seller_exchange_rate",        DoubleType,  true),
              StructField("vp_expose_domains",           IntegerType, true),
              StructField("vp_expose_categories",        IntegerType, true),
              StructField("vp_expose_pubs",              IntegerType, true),
              StructField("vp_expose_tag",               IntegerType, true),
              StructField("is_exclusive",                IntegerType, true),
              StructField("bidder_instance_id",          IntegerType, true),
              StructField("visibility_profile_id",       IntegerType, true),
              StructField("truncate_ip",                 IntegerType, true),
              StructField("device_id",                   IntegerType, true),
              StructField("carrier_id",                  IntegerType, true),
              StructField("creative_audit_status",       IntegerType, true),
              StructField("is_creative_hosted",          IntegerType, true),
              StructField("city",                        IntegerType, true),
              StructField("latitude",                    StringType,  true),
              StructField("longitude",                   StringType,  true),
              StructField("device_unique_id",            StringType,  true),
              StructField("supply_type",                 IntegerType, true),
              StructField("is_toolbar",                  IntegerType, true),
              StructField("deal_id",                     IntegerType, true),
              StructField("vp_bitmap",                   LongType,    true),
              StructField("ttl",                         IntegerType, true),
              StructField("view_detection_enabled",      IntegerType, true),
              StructField("ozone_id",                    IntegerType, true),
              StructField("is_performance",              IntegerType, true),
              StructField("sdk_version",                 StringType,  true),
              StructField("inventory_session_frequency", IntegerType, true),
              StructField("bid_price_type",              IntegerType, true),
              StructField("device_type",                 IntegerType, true),
              StructField("dma",                         IntegerType, true),
              StructField("postal",                      StringType,  true),
              StructField("package_id",                  IntegerType, true),
              StructField("spend_protection",            IntegerType, true),
              StructField("is_secure",                   IntegerType, true),
              StructField("estimated_view_rate",         DoubleType,  true),
              StructField("external_request_id",         StringType,  true),
              StructField("viewdef_definition_id_buyer_member",
                          IntegerType,
                          true
              ),
              StructField("spend_protection_pixel_id",      IntegerType, true),
              StructField("external_uid",                   StringType,  true),
              StructField("request_uuid",                   StringType,  true),
              StructField("mobile_app_instance_id",         IntegerType, true),
              StructField("traffic_source_code",            StringType,  true),
              StructField("stitch_group_id",                StringType,  true),
              StructField("deal_type",                      IntegerType, true),
              StructField("ym_floor_id",                    IntegerType, true),
              StructField("ym_bias_id",                     IntegerType, true),
              StructField("estimated_view_rate_over_total", DoubleType,  true),
              StructField("device_make_id",                 IntegerType, true),
              StructField("operating_system_family_id",     IntegerType, true),
              StructField(
                "tag_sizes",
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
                  Array(
                    StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
                  )
                ),
                true
              ),
              StructField(
                "buyer_transaction_def",
                StructType(
                  Array(
                    StructField("transaction_event",         IntegerType, true),
                    StructField("transaction_event_type_id", IntegerType, true)
                  )
                ),
                true
              ),
              StructField(
                "predicted_video_view_info",
                StructType(
                  Array(
                    StructField("iab_view_rate_over_measured",
                                DoubleType,
                                true
                    ),
                    StructField("iab_view_rate_over_total", DoubleType, true),
                    StructField("predicted_100pv50pd_video_view_rate",
                                DoubleType,
                                true
                    ),
                    StructField(
                      "predicted_100pv50pd_video_view_rate_over_total",
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
              StructField("seller_bid_currency_conversion_rate",
                          DoubleType,
                          true
              ),
              StructField("seller_bid_currency_code", StringType, true),
              StructField("is_prebid",                ByteType,   true),
              StructField("default_referrer_url",     StringType, true),
              StructField(
                "engagement_rates",
                ArrayType(
                  StructType(
                    Array(
                      StructField("engagement_rate_type",    IntegerType, true),
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
                    StructField("product_feed_id", IntegerType, true),
                    StructField("item_selection_strategy_id",
                                IntegerType,
                                true
                    ),
                    StructField("product_uuid", StringType, true)
                  )
                ),
                true
              ),
              StructField("ss_native_assembly_enabled", ByteType,   true),
              StructField("emp",                        DoubleType, true),
              StructField(
                "personal_identifiers",
                ArrayType(
                  StructType(
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
                ArrayType(
                  StructType(
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
          ),
          true
        )
      )
    )
  }

  def dw_imps_out(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      ArrayType(
        StructType(
          Array(
            StructField("date_time",                   LongType,    true),
            StructField("auction_id_64",               LongType,    true),
            StructField("user_id_64",                  LongType,    true),
            StructField("tag_id",                      IntegerType, true),
            StructField("venue_id",                    IntegerType, true),
            StructField("inventory_source_id",         IntegerType, true),
            StructField("session_frequency",           IntegerType, true),
            StructField("site_domain",                 StringType,  true),
            StructField("width",                       IntegerType, true),
            StructField("height",                      IntegerType, true),
            StructField("geo_country",                 StringType,  true),
            StructField("geo_region",                  StringType,  true),
            StructField("gender",                      StringType,  true),
            StructField("age",                         IntegerType, true),
            StructField("seller_member_id",            IntegerType, true),
            StructField("buyer_member_id",             IntegerType, true),
            StructField("creative_id",                 IntegerType, true),
            StructField("seller_currency",             StringType,  true),
            StructField("buyer_currency",              StringType,  true),
            StructField("buyer_bid",                   DoubleType,  true),
            StructField("buyer_spend",                 DoubleType,  true),
            StructField("ecp",                         DoubleType,  true),
            StructField("reserve_price",               DoubleType,  true),
            StructField("advertiser_id",               IntegerType, true),
            StructField("campaign_group_id",           IntegerType, true),
            StructField("campaign_id",                 IntegerType, true),
            StructField("creative_freq",               IntegerType, true),
            StructField("creative_rec",                IntegerType, true),
            StructField("is_learn",                    IntegerType, true),
            StructField("is_remarketing",              IntegerType, true),
            StructField("advertiser_frequency",        IntegerType, true),
            StructField("advertiser_recency",          IntegerType, true),
            StructField("user_group_id",               IntegerType, true),
            StructField("camp_dp_id",                  IntegerType, true),
            StructField("media_buy_id",                IntegerType, true),
            StructField("media_buy_cost",              DoubleType,  true),
            StructField("brand_id",                    IntegerType, true),
            StructField("cleared_direct",              IntegerType, true),
            StructField("clear_fees",                  DoubleType,  true),
            StructField("media_buy_rev_share_pct",     DoubleType,  true),
            StructField("revenue_value",               DoubleType,  true),
            StructField("pricing_type",                StringType,  true),
            StructField("can_convert",                 IntegerType, true),
            StructField("pub_rule_id",                 IntegerType, true),
            StructField("is_control",                  IntegerType, true),
            StructField("control_pct",                 DoubleType,  true),
            StructField("control_creative_id",         IntegerType, true),
            StructField("predicted_cpm",               DoubleType,  true),
            StructField("actual_bid",                  DoubleType,  true),
            StructField("site_id",                     IntegerType, true),
            StructField("content_category_id",         IntegerType, true),
            StructField("auction_service_fees",        DoubleType,  true),
            StructField("discrepancy_allowance",       DoubleType,  true),
            StructField("forex_allowance",             DoubleType,  true),
            StructField("creative_overage_fees",       DoubleType,  true),
            StructField("fold_position",               IntegerType, true),
            StructField("external_inv_id",             IntegerType, true),
            StructField("cadence_modifier",            DoubleType,  true),
            StructField("imp_type",                    IntegerType, true),
            StructField("advertiser_currency",         StringType,  true),
            StructField("advertiser_exchange_rate",    DoubleType,  true),
            StructField("ip_address",                  StringType,  true),
            StructField("publisher_id",                IntegerType, true),
            StructField("auction_service_deduction",   DoubleType,  true),
            StructField("insertion_order_id",          IntegerType, true),
            StructField("predict_type_rev",            IntegerType, true),
            StructField("predict_type_goal",           IntegerType, true),
            StructField("predict_type_cost",           IntegerType, true),
            StructField("booked_revenue_dollars",      DoubleType,  true),
            StructField("booked_revenue_adv_curr",     DoubleType,  true),
            StructField("commission_cpm",              DoubleType,  true),
            StructField("commission_revshare",         DoubleType,  true),
            StructField("serving_fees_cpm",            DoubleType,  true),
            StructField("serving_fees_revshare",       DoubleType,  true),
            StructField("user_tz_offset",              IntegerType, true),
            StructField("media_type",                  IntegerType, true),
            StructField("operating_system",            IntegerType, true),
            StructField("browser",                     IntegerType, true),
            StructField("language",                    IntegerType, true),
            StructField("publisher_currency",          StringType,  true),
            StructField("publisher_exchange_rate",     DoubleType,  true),
            StructField("media_cost_dollars_cpm",      DoubleType,  true),
            StructField("payment_type",                IntegerType, true),
            StructField("revenue_type",                IntegerType, true),
            StructField("seller_revenue_cpm",          DoubleType,  true),
            StructField("bidder_id",                   IntegerType, true),
            StructField("inv_code",                    StringType,  true),
            StructField("application_id",              StringType,  true),
            StructField("shadow_price",                DoubleType,  true),
            StructField("eap",                         DoubleType,  true),
            StructField("datacenter_id",               IntegerType, true),
            StructField("imp_blacklist_or_fraud",      IntegerType, true),
            StructField("vp_expose_domains",           IntegerType, true),
            StructField("vp_expose_categories",        IntegerType, true),
            StructField("vp_expose_pubs",              IntegerType, true),
            StructField("vp_expose_tag",               IntegerType, true),
            StructField("vp_expose_age",               IntegerType, true),
            StructField("vp_expose_gender",            IntegerType, true),
            StructField("inventory_url_id",            IntegerType, true),
            StructField("audit_type",                  IntegerType, true),
            StructField("is_exclusive",                IntegerType, true),
            StructField("truncate_ip",                 IntegerType, true),
            StructField("device_id",                   IntegerType, true),
            StructField("carrier_id",                  IntegerType, true),
            StructField("creative_audit_status",       IntegerType, true),
            StructField("is_creative_hosted",          IntegerType, true),
            StructField("seller_deduction",            DoubleType,  true),
            StructField("city",                        IntegerType, true),
            StructField("latitude",                    StringType,  true),
            StructField("longitude",                   StringType,  true),
            StructField("device_unique_id",            StringType,  true),
            StructField("package_id",                  IntegerType, true),
            StructField("targeted_segments",           StringType,  true),
            StructField("supply_type",                 IntegerType, true),
            StructField("is_toolbar",                  IntegerType, true),
            StructField("deal_id",                     IntegerType, true),
            StructField("vp_bitmap",                   LongType,    true),
            StructField("view_detection_enabled",      IntegerType, true),
            StructField("view_result",                 IntegerType, true),
            StructField("ozone_id",                    IntegerType, true),
            StructField("is_performance",              IntegerType, true),
            StructField("sdk_version",                 StringType,  true),
            StructField("inventory_session_frequency", IntegerType, true),
            StructField("device_type",                 IntegerType, true),
            StructField("dma",                         IntegerType, true),
            StructField("postal",                      StringType,  true),
            StructField("viewdef_definition_id",       IntegerType, true),
            StructField("viewdef_viewable",            IntegerType, true),
            StructField("view_measurable",             IntegerType, true),
            StructField("viewable",                    IntegerType, true),
            StructField("is_secure",                   IntegerType, true),
            StructField("view_non_measurable_reason",  IntegerType, true),
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
            StructField("bidder_instance_id",   IntegerType, true),
            StructField("campaign_group_freq",  IntegerType, true),
            StructField("campaign_group_rec",   IntegerType, true),
            StructField("insertion_order_freq", IntegerType, true),
            StructField("insertion_order_rec",  IntegerType, true),
            StructField("buyer_gender",         StringType,  true),
            StructField("buyer_age",            IntegerType, true),
            StructField("targeted_segment_list",
                        ArrayType(IntegerType, true),
                        true
            ),
            StructField("custom_model_id",            IntegerType, true),
            StructField("custom_model_last_modified", LongType,    true),
            StructField("custom_model_output_code",   StringType,  true),
            StructField("external_uid",               StringType,  true),
            StructField("request_uuid",               StringType,  true),
            StructField("mobile_app_instance_id",     IntegerType, true),
            StructField("traffic_source_code",        StringType,  true),
            StructField("external_request_id",        StringType,  true),
            StructField("stitch_group_id",            StringType,  true),
            StructField("deal_type",                  IntegerType, true),
            StructField("ym_floor_id",                IntegerType, true),
            StructField("ym_bias_id",                 IntegerType, true),
            StructField("bid_priority",               IntegerType, true),
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
            StructField("explore_disposition",        IntegerType, true),
            StructField("device_make_id",             IntegerType, true),
            StructField("operating_system_family_id", IntegerType, true),
            StructField(
              "tag_sizes",
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
            StructField("pricing_media_type",          IntegerType, true),
            StructField("buyer_trx_event_id",          IntegerType, true),
            StructField("seller_trx_event_id",         IntegerType, true),
            StructField("revenue_auction_event_type",  IntegerType, true),
            StructField("is_prebid",                   ByteType,    true),
            StructField("is_unit_of_trx",              ByteType,    true),
            StructField("imps_for_budget_caps_pacing", IntegerType, true),
            StructField("auction_timestamp",           LongType,    true),
            StructField("two_phase_reduction_applied", ByteType,    true),
            StructField("region_id",                   IntegerType, true),
            StructField("media_company_id",            IntegerType, true),
            StructField("trade_agreement_id",          IntegerType, true),
            StructField(
              "personal_data",
              StructType(
                Array(
                  StructField("user_id_64",       LongType,   true),
                  StructField("device_unique_id", StringType, true),
                  StructField("external_uid",     StringType, true),
                  StructField("ip_address",       BinaryType, true),
                  StructField(
                    "crossdevice_group",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", LongType,    true)
                      )
                    ),
                    true
                  ),
                  StructField("latitude",                 DoubleType,  true),
                  StructField("longitude",                DoubleType,  true),
                  StructField("ipv6_address",             BinaryType,  true),
                  StructField("subject_to_gdpr",          ByteType,    true),
                  StructField("geo_country",              StringType,  true),
                  StructField("gdpr_consent_string",      StringType,  true),
                  StructField("preempt_ip_address",       BinaryType,  true),
                  StructField("device_type",              IntegerType, true),
                  StructField("device_make_id",           IntegerType, true),
                  StructField("device_model_id",          IntegerType, true),
                  StructField("new_user_id_64",           LongType,    true),
                  StructField("is_service_provider_mode", ByteType,    true),
                  StructField("is_personal_info_sale",    ByteType,    true)
                )
              ),
              true
            ),
            StructField(
              "anonymized_user_info",
              StructType(Array(StructField("user_id", BinaryType, true))),
              true
            ),
            StructField("gdpr_consent_cookie", StringType, true),
            StructField("additional_clearing_events",
                        ArrayType(IntegerType, true),
                        true
            ),
            StructField("fx_rate_snapshot_id", IntegerType, true),
            StructField("crossdevice_group_anon",
                        StructType(
                          Array(StructField("graph_id", IntegerType, true),
                                StructField("group_id", BinaryType,  true)
                          )
                        ),
                        true
            ),
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
            StructField("revenue_event_type_id",    IntegerType, true),
            StructField("buyer_trx_event_type_id",  IntegerType, true),
            StructField("seller_trx_event_type_id", IntegerType, true),
            StructField("external_creative_id",     StringType,  true),
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
            StructField("bidder_seat_id",                IntegerType, true),
            StructField("is_whiteops_scanned",           ByteType,    true),
            StructField("default_referrer_url",          StringType,  true),
            StructField("is_curated",                    ByteType,    true),
            StructField("curator_member_id",             IntegerType, true),
            StructField("total_partner_fees_microcents", LongType,    true),
            StructField("net_buyer_spend",               DoubleType,  true),
            StructField("is_prebid_server",              ByteType,    true),
            StructField("cold_start_price_type",         IntegerType, true),
            StructField("discovery_state",               IntegerType, true),
            StructField("billing_period_id",             IntegerType, true),
            StructField("flight_id",                     IntegerType, true),
            StructField("split_id",                      IntegerType, true),
            StructField("net_media_cost_dollars_cpm",    DoubleType,  true),
            StructField("total_data_costs_microcents",   LongType,    true),
            StructField("total_profit_microcents",       LongType,    true),
            StructField("targeted_crossdevice_graph_id", IntegerType, true),
            StructField("discovery_prediction",          DoubleType,  true),
            StructField("campaign_group_type_id",        IntegerType, true),
            StructField("hb_source",                     IntegerType, true),
            StructField("external_campaign_id",          StringType,  true),
            StructField(
              "excluded_targeted_segment_details",
              ArrayType(
                StructType(Array(StructField("segment_id", IntegerType, true))),
                true
              ),
              true
            ),
            StructField("trust_id",                        StringType, true),
            StructField("predicted_kpi_event_rate",        DoubleType, true),
            StructField("has_crossdevice_reach_extension", ByteType,   true),
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
            StructField("total_segment_data_costs_microcents", LongType,    true),
            StructField("total_feature_costs_microcents",      LongType,    true),
            StructField("counterparty_ruleset_type",           IntegerType, true),
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
            StructField("buyer_line_item_currency",       StringType,  true),
            StructField("deal_line_item_currency",        StringType,  true),
            StructField("measurement_fee_usd",            DoubleType,  true),
            StructField("measurement_provider_member_id", IntegerType, true),
            StructField("offline_attribution_provider_member_id",
                        IntegerType,
                        true
            ),
            StructField("offline_attribution_cost_usd_cpm", DoubleType,  true),
            StructField("pred_info",                        IntegerType, true),
            StructField("imp_rejecter_do_auction",          ByteType,    true),
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
            StructField("imp_rejecter_applied", ByteType,   true),
            StructField("ip_derived_latitude",  DoubleType, true),
            StructField("ip_derived_longitude", DoubleType, true),
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
            StructField("postal_code_ext_id",   IntegerType, true),
            StructField("ecpm_conversion_rate", DoubleType,  true),
            StructField("is_residential_ip",    ByteType,    true),
            StructField("hashed_ip",            StringType,  true),
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
            StructField("estimated_audience_imps",      DoubleType, true),
            StructField("audience_imps",                DoubleType, true),
            StructField("district_postal_code_lists",
                        ArrayType(IntegerType, true),
                        true
            ),
            StructField("bidding_host_id",           IntegerType, true),
            StructField("buyer_dpvp_bitmap",         LongType,    true),
            StructField("seller_dpvp_bitmap",        LongType,    true),
            StructField("browser_code_id",           IntegerType, true),
            StructField("is_prebid_server_included", IntegerType, true),
            StructField("feature_tests_bitmap",      IntegerType, true),
            StructField("private_auction_eligible",  ByteType,    true),
            StructField("chrome_traffic_label",      IntegerType, true),
            StructField("is_private_auction",        ByteType,    true),
            StructField("creative_media_subtype_id", IntegerType, true),
            StructField("allowed_media_types",
                        ArrayType(IntegerType, true),
                        true
            )
          )
        ),
        true
      )
    )
  }

  def stage_seen_out(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    lit(null).cast(
      ArrayType(
        StructType(
          Array(
            StructField("date_time",                      LongType,    true),
            StructField("auction_id_64",                  LongType,    true),
            StructField("seller_member_id",               IntegerType, true),
            StructField("buyer_member_id",                IntegerType, true),
            StructField("width",                          IntegerType, true),
            StructField("height",                         IntegerType, true),
            StructField("publisher_id",                   IntegerType, true),
            StructField("site_id",                        IntegerType, true),
            StructField("tag_id",                         IntegerType, true),
            StructField("gender",                         StringType,  true),
            StructField("geo_country",                    StringType,  true),
            StructField("inventory_source_id",            IntegerType, true),
            StructField("imp_type",                       IntegerType, true),
            StructField("is_dw",                          IntegerType, true),
            StructField("bidder_id",                      IntegerType, true),
            StructField("sampling_pct",                   DoubleType,  true),
            StructField("seller_revenue",                 DoubleType,  true),
            StructField("buyer_spend",                    DoubleType,  true),
            StructField("cleared_direct",                 IntegerType, true),
            StructField("creative_overage_fees",          DoubleType,  true),
            StructField("auction_service_fees",           DoubleType,  true),
            StructField("clear_fees",                     DoubleType,  true),
            StructField("discrepancy_allowance",          DoubleType,  true),
            StructField("forex_allowance",                DoubleType,  true),
            StructField("auction_service_deduction",      DoubleType,  true),
            StructField("content_category_id",            IntegerType, true),
            StructField("datacenter_id",                  IntegerType, true),
            StructField("imp_bid_on",                     IntegerType, true),
            StructField("ecp",                            DoubleType,  true),
            StructField("eap",                            DoubleType,  true),
            StructField("buyer_currency",                 StringType,  true),
            StructField("buyer_spend_buyer_currency",     DoubleType,  true),
            StructField("seller_currency",                StringType,  true),
            StructField("seller_revenue_seller_currency", DoubleType,  true),
            StructField("vp_expose_pubs",                 IntegerType, true),
            StructField("seller_deduction",               DoubleType,  true),
            StructField("supply_type",                    IntegerType, true),
            StructField("is_delivered",                   IntegerType, true),
            StructField("buyer_bid_bucket",               IntegerType, true),
            StructField("device_id",                      IntegerType, true),
            StructField("user_id_64",                     LongType,    true),
            StructField("cookie_age",                     IntegerType, true),
            StructField("ip_address",                     StringType,  true),
            StructField("imp_blacklist_or_fraud",         IntegerType, true),
            StructField("site_domain",                    StringType,  true),
            StructField("view_measurable",                IntegerType, true),
            StructField("viewable",                       IntegerType, true),
            StructField("call_type",                      StringType,  true),
            StructField("deal_id",                        IntegerType, true),
            StructField("application_id",                 StringType,  true),
            StructField("imp_date_time",                  LongType,    true),
            StructField("media_type",                     IntegerType, true),
            StructField("pub_rule_id",                    IntegerType, true),
            StructField("venue_id",                       IntegerType, true),
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
            StructField("buyer_bid",          DoubleType, true),
            StructField("preempt_ip_address", StringType, true),
            StructField(
              "seller_transaction_def",
              StructType(
                Array(
                  StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
                )
              ),
              true
            ),
            StructField(
              "buyer_transaction_def",
              StructType(
                Array(
                  StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
                )
              ),
              true
            ),
            StructField("is_imp_rejecter_applied",    ByteType,    true),
            StructField("imp_rejecter_do_auction",    ByteType,    true),
            StructField("audit_type",                 IntegerType, true),
            StructField("browser",                    IntegerType, true),
            StructField("device_type",                IntegerType, true),
            StructField("geo_region",                 StringType,  true),
            StructField("language",                   IntegerType, true),
            StructField("operating_system",           IntegerType, true),
            StructField("operating_system_family_id", IntegerType, true),
            StructField("allowed_media_types",
                        ArrayType(IntegerType, true),
                        true
            ),
            StructField("imp_biddable",                ByteType,    true),
            StructField("imp_ignored",                 ByteType,    true),
            StructField("user_group_id",               IntegerType, true),
            StructField("inventory_url_id",            IntegerType, true),
            StructField("vp_expose_domains",           IntegerType, true),
            StructField("visibility_profile_id",       IntegerType, true),
            StructField("is_exclusive",                IntegerType, true),
            StructField("truncate_ip",                 IntegerType, true),
            StructField("creative_id",                 IntegerType, true),
            StructField("buyer_exchange_rate",         DoubleType,  true),
            StructField("vp_expose_tag",               IntegerType, true),
            StructField("view_result",                 IntegerType, true),
            StructField("age",                         IntegerType, true),
            StructField("brand_id",                    IntegerType, true),
            StructField("carrier_id",                  IntegerType, true),
            StructField("city",                        IntegerType, true),
            StructField("dma",                         IntegerType, true),
            StructField("device_unique_id",            StringType,  true),
            StructField("latitude",                    StringType,  true),
            StructField("longitude",                   StringType,  true),
            StructField("postal",                      StringType,  true),
            StructField("sdk_version",                 StringType,  true),
            StructField("pricing_media_type",          IntegerType, true),
            StructField("traffic_source_code",         StringType,  true),
            StructField("is_prebid",                   ByteType,    true),
            StructField("is_unit_of_buyer_trx",        ByteType,    true),
            StructField("is_unit_of_seller_trx",       ByteType,    true),
            StructField("two_phase_reduction_applied", ByteType,    true),
            StructField("region_id",                   IntegerType, true),
            StructField(
              "personal_data",
              StructType(
                Array(
                  StructField("user_id_64",       LongType,   true),
                  StructField("device_unique_id", StringType, true),
                  StructField("external_uid",     StringType, true),
                  StructField("ip_address",       BinaryType, true),
                  StructField(
                    "crossdevice_group",
                    StructType(
                      Array(StructField("graph_id", IntegerType, true),
                            StructField("group_id", LongType,    true)
                      )
                    ),
                    true
                  ),
                  StructField("latitude",                 DoubleType,  true),
                  StructField("longitude",                DoubleType,  true),
                  StructField("ipv6_address",             BinaryType,  true),
                  StructField("subject_to_gdpr",          ByteType,    true),
                  StructField("geo_country",              StringType,  true),
                  StructField("gdpr_consent_string",      StringType,  true),
                  StructField("preempt_ip_address",       BinaryType,  true),
                  StructField("device_type",              IntegerType, true),
                  StructField("device_make_id",           IntegerType, true),
                  StructField("device_model_id",          IntegerType, true),
                  StructField("new_user_id_64",           LongType,    true),
                  StructField("is_service_provider_mode", ByteType,    true),
                  StructField("is_personal_info_sale",    ByteType,    true)
                )
              ),
              true
            ),
            StructField(
              "anonymized_user_info",
              StructType(Array(StructField("user_id", BinaryType, true))),
              true
            ),
            StructField("gdpr_consent_cookie",        StringType,  true),
            StructField("external_creative_id",       StringType,  true),
            StructField("subject_to_gdpr",            ByteType,    true),
            StructField("fx_rate_snapshot_id",        IntegerType, true),
            StructField("view_detection_enabled",     IntegerType, true),
            StructField("seller_exchange_rate",       DoubleType,  true),
            StructField("browser_code_id",            IntegerType, true),
            StructField("is_prebid_server_included",  IntegerType, true),
            StructField("bidder_seat_id",             IntegerType, true),
            StructField("default_referrer_url",       StringType,  true),
            StructField("pred_info",                  IntegerType, true),
            StructField("curated_deal_id",            IntegerType, true),
            StructField("deal_type",                  IntegerType, true),
            StructField("primary_height",             IntegerType, true),
            StructField("primary_width",              IntegerType, true),
            StructField("curator_member_id",          IntegerType, true),
            StructField("instance_id",                IntegerType, true),
            StructField("hb_source",                  IntegerType, true),
            StructField("from_imps_seen",             ByteType,    true),
            StructField("external_campaign_id",       StringType,  true),
            StructField("ss_native_assembly_enabled", ByteType,    true),
            StructField("uid_source",                 IntegerType, true),
            StructField("video_context",              IntegerType, true),
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
            StructField("user_tz_offset",             IntegerType, true),
            StructField("external_bidrequest_id",     LongType,    true),
            StructField("external_bidrequest_imp_id", LongType,    true),
            StructField("ym_floor_id",                IntegerType, true),
            StructField("ym_bias_id",                 IntegerType, true),
            StructField("openrtb_req_subdomain",      StringType,  true)
          )
        ),
        true
      )
    )
  }

  def dw_views(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(is_not_null(col("log_dw_view")), array(col("log_dw_view"))).otherwise(
      lit(null).cast(
        ArrayType(
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
              StructField("use_revenue_info",     ByteType,   true),
              StructField("is_deferred",          ByteType,   true),
              StructField("ecpm_conversion_rate", DoubleType, true)
            )
          ),
          true
        )
      )
    )
  }

  def bids(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_dw_bid")),
      filter(
        array(
          col("log_dw_bid"),
          when(
            is_not_null(col("log_dw_bid_last")).and(
              coalesce(
                col("log_impbus_impressions_pricing_count").cast(IntegerType),
                lit(1)
              ).cast(IntegerType) > lit(1)
            ),
            col("log_dw_bid_last")
          ),
          when(is_not_null(col("log_dw_bid_deal")), col("log_dw_bid_deal")),
          when(is_not_null(col("log_dw_bid_curator")),
               col("log_dw_bid_curator")
          )
        ),
        xx => is_not_null(xx)
      )
    ).otherwise(
      lit(null).cast(
        ArrayType(
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
          ),
          true
        )
      )
    )
  }

}
