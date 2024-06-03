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

object Reformat_TRAN_Router_ReformatterReformat_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        when(coalesce(col("log_impbus_impressions.ttl").cast(IntegerType),
                      lit(0)
             ).cast(IntegerType) >= lit(3600),
             col("date_time").cast(LongType) + lit(5) * lit(3600)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      f_preempt_over_impression_non_zero_waterfall(
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).cast(IntegerType).as("width"),
      f_preempt_over_impression_non_zero_waterfall(
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).cast(IntegerType).as("height"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      when(string_compare(col("log_impbus_impressions.gender"),
                          lit("u")
           ) =!= lit(0),
           col("log_impbus_impressions.gender")
      ).as("gender"),
      coalesce(
        when(string_compare(col("log_impbus_impressions.geo_country"),
                            lit("--")
             ) =!= lit(0),
             col("log_impbus_impressions.geo_country")
        ),
        when(col("log_impbus_impressions.geo_country").isNull, lit("US"))
      ).as("geo_country"),
      when(col("log_impbus_impressions.inventory_source_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_source_id").cast(IntegerType)
      ).cast(IntegerType).as("inventory_source_id"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      when(col("is_dw").cast(IntegerType) =!= lit(0),
           col("is_dw").cast(IntegerType)
      ).as("is_dw"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")), lit(0)).cast(IntegerType),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      lit(null).cast(DoubleType).as("sampling_pct"),
      lit(null).cast(DoubleType).as("seller_revenue"),
      lit(null).cast(DoubleType).as("buyer_spend"),
      when(col("log_impbus_impressions.cleared_direct").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.cleared_direct").cast(IntegerType)
      ).cast(IntegerType).as("cleared_direct"),
      lit(null).cast(DoubleType).as("creative_overage_fees"),
      lit(null).cast(DoubleType).as("auction_service_fees"),
      lit(null).cast(DoubleType).as("clear_fees"),
      lit(null).cast(DoubleType).as("discrepancy_allowance"),
      lit(null).cast(DoubleType).as("forex_allowance"),
      lit(null).cast(DoubleType).as("auction_service_deduction"),
      when(col("log_impbus_impressions.content_category_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.content_category_id").cast(IntegerType)
      ).cast(IntegerType).as("content_category_id"),
      coalesce(
        when(col("log_impbus_impressions.datacenter_id").cast(
               IntegerType
             ) =!= lit(0),
             col("log_impbus_impressions.datacenter_id").cast(IntegerType)
        ).cast(IntegerType),
        lit(1).cast(IntegerType)
      ).as("datacenter_id"),
      when(
        col("log_impbus_impressions.imp_bid_on").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.imp_bid_on").cast(IntegerType)
      ).cast(IntegerType).as("imp_bid_on"),
      when(col("log_impbus_impressions.ecp") =!= lit(0),
           col("log_impbus_impressions.ecp")
      ).cast(DoubleType).as("ecp"),
      when(col("log_impbus_impressions.eap") =!= lit(0),
           col("log_impbus_impressions.eap")
      ).cast(DoubleType).as("eap"),
      when(string_compare(col("log_impbus_impressions.buyer_currency"),
                          lit("USD")
           ) =!= lit(0),
           col("log_impbus_impressions.buyer_currency")
      ).as("buyer_currency"),
      lit(null).cast(DoubleType).as("buyer_spend_buyer_currency"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("USD")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      lit(null).cast(DoubleType).as("seller_revenue_seller_currency"),
      lit(null).cast(IntegerType).as("vp_expose_pubs"),
      lit(null).cast(DoubleType).as("seller_deduction"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).cast(IntegerType).as("supply_type"),
      when(
        col("log_impbus_impressions.is_delivered").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_delivered").cast(IntegerType)
      ).as("is_delivered"),
      f_get_buyer_bid_bucket(
        coalesce(col("log_impbus_preempt.buyer_bid"),
                 col("log_impbus_impressions.buyer_bid"),
                 lit(0)
        ).cast(DoubleType)
      ).cast(IntegerType).as("buyer_bid_bucket"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).cast(IntegerType).as("device_id"),
      when(col("log_impbus_impressions.user_id_64").cast(LongType) =!= lit(0),
           col("log_impbus_impressions.user_id_64").cast(LongType)
      ).cast(LongType).as("user_id_64"),
      when(
        col("log_impbus_impressions.cookie_age").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.cookie_age").cast(IntegerType)
      ).cast(IntegerType).as("cookie_age"),
      coalesce(coalesce(col("log_impbus_preempt.ip_address"),
                        col("log_impbus_impressions.ip_address")
               ),
               lit("---")
      ).as("ip_address"),
      when(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType)
      ).cast(IntegerType).as("imp_blacklist_or_fraud"),
      coalesce(
        when(
          is_not_null(
            col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
          ).and(
              col("log_impbus_impressions.inventory_url_id")
                .cast(IntegerType) =!= lit(0)
            )
            .and(
              is_not_null(
                lookup_inventory_url_by_id(
                  lit("inventory_url_by_id"),
                  col("log_impbus_impressions.inventory_url_id")
                    .cast(IntegerType)
                ).getField("inventory_url")
              )
            ),
          string_substring(
            lookup_inventory_url_by_id(
              lit("inventory_url_by_id"),
              col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
            ).getField("inventory_url"),
            lit(1),
            lit(98)
          )
        ).otherwise(lit(null).cast(StringType)),
        when(
          is_not_null(col("log_impbus_impressions.site_domain")).and(
            string_compare(lit("---"),
                           col("log_impbus_impressions.site_domain")
            ) =!= lit(0)
          ),
          string_substring(col("log_impbus_impressions.site_domain"),
                           lit(1),
                           lit(98)
          )
        )
      ).as("site_domain"),
      lit(null).cast(IntegerType).as("view_measurable"),
      lit(null).cast(IntegerType).as("viewable"),
      coalesce(col("log_impbus_impressions.call_type"), lit("/openrtb2"))
        .as("call_type"),
      when(col("log_impbus_impressions.deal_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.deal_id").cast(IntegerType)
      ).cast(IntegerType).as("deal_id"),
      when(
        is_not_null(col("log_impbus_impressions.application_id")).and(
          string_compare(lit("---"),
                         col("log_impbus_impressions.application_id")
          ) =!= lit(0)
        ),
        re_match_replace_all(
          string_cleanse(
            col("log_impbus_impressions.application_id").cast(StringType),
            lit("."),
            lit("utf-16")
          ),
          lit("\\s+"),
          lit("")
        )
      ).otherwise(col("log_impbus_impressions.application_id"))
        .as("application_id"),
      col("date_time").cast(LongType).as("imp_date_time"),
      coalesce(
        coalesce(col("log_impbus_preempt.media_type").cast(IntegerType),
                 col("log_impbus_impressions.media_type").cast(IntegerType)
        ).cast(IntegerType),
        lit(1).cast(IntegerType)
      ).as("media_type"),
      when(
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.pub_rule_id").cast(IntegerType)
      ).cast(IntegerType).as("pub_rule_id"),
      when(col("log_impbus_impressions.venue_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.venue_id").cast(IntegerType)
      ).cast(IntegerType).as("venue_id"),
      lit(null)
        .cast(
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
          )
        )
        .as("buyer_charges"),
      lit(null)
        .cast(
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
          )
        )
        .as("seller_charges"),
      when(
        coalesce(col("log_impbus_preempt.buyer_bid"),
                 col("log_impbus_impressions.buyer_bid"),
                 lit(0)
        ).cast(DoubleType) =!= lit(0),
        coalesce(col("log_impbus_preempt.buyer_bid"),
                 col("log_impbus_impressions.buyer_bid"),
                 lit(0)
        ).cast(DoubleType)
      ).cast(DoubleType).as("buyer_bid"),
      lit(null).cast(StringType).as("preempt_ip_address"),
      struct(lit(1).as("transaction_event"),
             lit(null).as("transaction_event_type_id")
      ).as("seller_transaction_def"),
      struct(lit(1).as("transaction_event"),
             lit(null).as("transaction_event_type_id")
      ).as("buyer_transaction_def"),
      when(col("log_impbus_impressions.is_imp_rejecter_applied").cast(
             ByteType
           ) =!= lit(0),
           col("log_impbus_impressions.is_imp_rejecter_applied").cast(ByteType)
      ).cast(ByteType).as("is_imp_rejecter_applied"),
      col("log_impbus_impressions.imp_rejecter_do_auction")
        .cast(ByteType)
        .as("imp_rejecter_do_auction"),
      coalesce(
        when(
          is_not_null(
            col("log_impbus_impressions.audit_type").cast(IntegerType)
          ).and(
            col("log_impbus_impressions.audit_type")
              .cast(IntegerType) =!= lit(0)
          ),
          col("log_impbus_impressions.audit_type").cast(IntegerType)
        ).cast(IntegerType),
        when(col("log_impbus_impressions.audit_type").isNull, lit(2))
          .cast(IntegerType)
      ).as("audit_type"),
      lit(null).cast(IntegerType).as("browser"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).cast(IntegerType).as("device_type"),
      lit(null).cast(StringType).as("geo_region"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).cast(IntegerType).as("language"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).cast(IntegerType).as("operating_system"),
      when(
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        ) =!= lit(1),
        col("log_impbus_impressions.operating_system_family_id").cast(
          IntegerType
        )
      ).cast(IntegerType).as("operating_system_family_id"),
      col("log_impbus_impressions.allowed_media_types")
        .as("allowed_media_types"),
      (coalesce(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType),
        lit(0)
      ).cast(IntegerType) === lit(0))
        .and(
          coalesce(col("log_impbus_impressions.imp_rejecter_do_auction")
                     .cast(ByteType),
                   lit(1)
          ).cast(IntegerType) === lit(1)
        )
        .cast(ByteType)
        .as("imp_biddable"),
      (coalesce(
        col("log_impbus_impressions.imp_blacklist_or_fraud").cast(IntegerType),
        lit(0)
      ).cast(IntegerType) === lit(0))
        .and(
          coalesce(col("log_impbus_impressions.imp_rejecter_do_auction")
                     .cast(ByteType),
                   lit(1)
          ).cast(IntegerType) === lit(0)
        )
        .cast(ByteType)
        .as("imp_ignored"),
      coalesce(
        when(col("log_impbus_impressions.user_group_id").cast(
               IntegerType
             ) =!= lit(0),
             col("log_impbus_impressions.user_group_id").cast(IntegerType)
        ).cast(IntegerType),
        when(col("log_impbus_impressions.user_group_id").isNull, lit(-1))
          .cast(IntegerType)
      ).as("user_group_id"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).cast(IntegerType).as("inventory_url_id"),
      when(col("log_impbus_impressions.vp_expose_domains").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.vp_expose_domains").cast(IntegerType)
      ).cast(IntegerType).as("vp_expose_domains"),
      when(col("log_impbus_impressions.visibility_profile_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.visibility_profile_id").cast(IntegerType)
      ).cast(IntegerType).as("visibility_profile_id"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).cast(IntegerType).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).cast(IntegerType).as("truncate_ip"),
      lit(null).cast(IntegerType).as("creative_id"),
      lit(null).cast(DoubleType).as("buyer_exchange_rate"),
      lit(null).cast(IntegerType).as("vp_expose_tag"),
      lit(null).cast(IntegerType).as("view_result"),
      lit(null).cast(IntegerType).as("age"),
      lit(null).cast(IntegerType).as("brand_id"),
      lit(null).cast(IntegerType).as("carrier_id"),
      lit(null).cast(IntegerType).as("city"),
      lit(null).cast(IntegerType).as("dma"),
      lit(null).cast(StringType).as("device_unique_id"),
      lit(null).cast(StringType).as("latitude"),
      lit(null).cast(StringType).as("longitude"),
      lit(null).cast(StringType).as("postal"),
      col("log_impbus_impressions.sdk_version").as("sdk_version"),
      lit(null).cast(IntegerType).as("pricing_media_type"),
      col("log_impbus_impressions.traffic_source_code")
        .as("traffic_source_code"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(ByteType)
      ).cast(ByteType).as("is_prebid"),
      lit(null).cast(ByteType).as("is_unit_of_buyer_trx"),
      lit(null).cast(ByteType).as("is_unit_of_seller_trx"),
      lit(null).cast(ByteType).as("two_phase_reduction_applied"),
      when(col("log_impbus_impressions.region_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.region_id").cast(IntegerType)
      ).cast(IntegerType).as("region_id"),
      lit(null)
        .cast(
          StructType(
            Array(
              StructField("user_id_64",       LongType,   true),
              StructField("device_unique_id", StringType, true),
              StructField("external_uid",     StringType, true),
              StructField("ip_address",       BinaryType, true),
              StructField("crossdevice_group",
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
          )
        )
        .as("personal_data"),
      col("log_impbus_impressions.anonymized_user_info")
        .as("anonymized_user_info"),
      col("log_impbus_impressions.gdpr_consent_cookie")
        .as("gdpr_consent_cookie"),
      lit(null).cast(StringType).as("external_creative_id"),
      when(
        col("log_impbus_impressions.subject_to_gdpr").cast(ByteType) =!= lit(0),
        col("log_impbus_impressions.subject_to_gdpr").cast(ByteType)
      ).cast(ByteType).as("subject_to_gdpr"),
      when(col("log_impbus_impressions.fx_rate_snapshot_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.fx_rate_snapshot_id").cast(IntegerType)
      ).cast(IntegerType).as("fx_rate_snapshot_id"),
      lit(null).cast(IntegerType).as("view_detection_enabled"),
      lit(null).cast(DoubleType).as("seller_exchange_rate"),
      when(col("log_impbus_impressions.browser_code_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.browser_code_id").cast(IntegerType)
      ).cast(IntegerType).as("browser_code_id"),
      when(
        col("log_impbus_impressions.is_prebid_server_included").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.is_prebid_server_included").cast(
          IntegerType
        )
      ).cast(IntegerType).as("is_prebid_server_included"),
      coalesce(
        col("log_impbus_preempt.seat_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")), lit(0)).cast(IntegerType),
        col("log_impbus_impressions.seat_id").cast(IntegerType),
        lit(0).cast(IntegerType)
      ).as("bidder_seat_id"),
      lit(null).cast(StringType).as("default_referrer_url"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).cast(IntegerType).as("pred_info"),
      lit(null).cast(IntegerType).as("curated_deal_id"),
      lit(null).cast(IntegerType).as("deal_type"),
      coalesce(
        when(size(col("log_impbus_impressions.tag_sizes")) > lit(0),
             element_at(col("log_impbus_impressions.tag_sizes"), 1).getField(
               "height"
             )
        ).cast(IntegerType),
        f_preempt_over_impression_non_zero_waterfall(
          col("log_impbus_impressions.height").cast(IntegerType),
          col("log_impbus_preempt.height").cast(IntegerType)
        ).cast(IntegerType),
        lit(50).cast(IntegerType)
      ).as("primary_height"),
      coalesce(
        when(size(col("log_impbus_impressions.tag_sizes")) > lit(0),
             element_at(col("log_impbus_impressions.tag_sizes"), 1).getField(
               "width"
             )
        ).cast(IntegerType),
        f_preempt_over_impression_non_zero_waterfall(
          col("log_impbus_impressions.width").cast(IntegerType),
          col("log_impbus_preempt.width").cast(IntegerType)
        ).cast(IntegerType),
        lit(300).cast(IntegerType)
      ).as("primary_width"),
      lit(null).cast(IntegerType).as("curator_member_id"),
      lit(null).cast(IntegerType).as("instance_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).cast(IntegerType).as("hb_source"),
      lit(null).cast(ByteType).as("from_imps_seen"),
      lit(null).cast(StringType).as("external_campaign_id"),
      when(
        col("log_impbus_impressions.ss_native_assembly_enabled").cast(
          ByteType
        ) =!= lit(0),
        col("log_impbus_impressions.ss_native_assembly_enabled").cast(ByteType)
      ).cast(ByteType).as("ss_native_assembly_enabled"),
      col("log_impbus_impressions.uid_source")
        .cast(IntegerType)
        .as("uid_source"),
      coalesce(
        when(col("video_slot.video_context").cast(IntegerType) =!= lit(0),
             col("video_slot.video_context").cast(IntegerType)
        ).cast(IntegerType),
        when(
          col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
          lookup("sup_placement_video_attributes_pb",
                 col("log_impbus_impressions.tag_id").cast(IntegerType)
          ).getField("video_context")
        ).cast(IntegerType)
      ).as("video_context"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_impbus_impressions.personal_identifiers_experimental")
        .as("personal_identifiers_experimental"),
      col("log_impbus_impressions.user_tz_offset")
        .cast(IntegerType)
        .as("user_tz_offset"),
      lit(null).cast(LongType).as("external_bidrequest_id"),
      lit(null).cast(LongType).as("external_bidrequest_imp_id"),
      lit(null).cast(IntegerType).as("ym_floor_id"),
      lit(null).cast(IntegerType).as("ym_bias_id"),
      coalesce(
        when(is_not_null(col("log_impbus_impressions.openrtb_req_subdomain")),
             col("log_impbus_impressions.openrtb_req_subdomain")
        ),
        lit(null).cast(StringType)
      ).as("openrtb_req_subdomain")
    )

}
