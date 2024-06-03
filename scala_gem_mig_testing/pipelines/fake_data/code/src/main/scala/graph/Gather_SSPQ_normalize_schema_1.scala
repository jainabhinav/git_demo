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

object Gather_SSPQ_normalize_schema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("postal").cast(StringType).as("postal"),
      col("curator_member_id").cast(IntegerType).as("curator_member_id"),
      col("discrepancy_allowance").cast(DoubleType).as("discrepancy_allowance"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("city").cast(IntegerType).as("city"),
      col("device_id").cast(IntegerType).as("device_id"),
      col("allowed_media_types")
        .cast(ArrayType(IntegerType, true))
        .as("allowed_media_types"),
      col("external_bidrequest_imp_id")
        .cast(LongType)
        .as("external_bidrequest_imp_id"),
      col("operating_system").cast(IntegerType).as("operating_system"),
      col("buyer_bid").cast(DoubleType).as("buyer_bid"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      col("latitude").cast(StringType).as("latitude"),
      col("is_dw").cast(IntegerType).as("is_dw"),
      col("hb_source").cast(IntegerType).as("hb_source"),
      col("view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("geo_country").cast(StringType).as("geo_country"),
      col("inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
      struct(col("anonymized_user_info.user_id").as("user_id"))
        .cast(StructType(Array(StructField("user_id", BinaryType, true))))
        .as("anonymized_user_info"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("eap").cast(DoubleType).as("eap"),
      col("is_prebid_server_included")
        .cast(IntegerType)
        .as("is_prebid_server_included"),
      col("external_campaign_id").cast(StringType).as("external_campaign_id"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("creative_overage_fees").cast(DoubleType).as("creative_overage_fees"),
      col("traffic_source_code").cast(StringType).as("traffic_source_code"),
      col("browser").cast(IntegerType).as("browser"),
      col("buyer_exchange_rate").cast(DoubleType).as("buyer_exchange_rate"),
      col("call_type").cast(StringType).as("call_type"),
      col("forex_allowance").cast(DoubleType).as("forex_allowance"),
      col("pred_info").cast(IntegerType).as("pred_info"),
      col("external_creative_id").cast(StringType).as("external_creative_id"),
      col("geo_region").cast(StringType).as("geo_region"),
      col("supply_type").cast(IntegerType).as("supply_type"),
      col("primary_height").cast(IntegerType).as("primary_height"),
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
      struct(
        col("seller_charges.rate_card_id").as("rate_card_id"),
        col("seller_charges.member_id").as("member_id"),
        col("seller_charges.is_dw").as("is_dw"),
        col("seller_charges.pricing_terms").as("pricing_terms"),
        col("seller_charges.fx_margin_rate_id").as("fx_margin_rate_id"),
        col("seller_charges.marketplace_owner_id").as("marketplace_owner_id"),
        col("seller_charges.virtual_marketplace_id").as(
          "virtual_marketplace_id"
        ),
        col("seller_charges.amino_enabled").as("amino_enabled")
      ).cast(
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
      col("height").cast(IntegerType).as("height"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      struct(
        col("buyer_charges.rate_card_id").as("rate_card_id"),
        col("buyer_charges.member_id").as("member_id"),
        col("buyer_charges.is_dw").as("is_dw"),
        col("buyer_charges.pricing_terms").as("pricing_terms"),
        col("buyer_charges.fx_margin_rate_id").as("fx_margin_rate_id"),
        col("buyer_charges.marketplace_owner_id").as("marketplace_owner_id"),
        col("buyer_charges.virtual_marketplace_id").as(
          "virtual_marketplace_id"
        ),
        col("buyer_charges.amino_enabled").as("amino_enabled")
      ).cast(
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
      col("brand_id").cast(IntegerType).as("brand_id"),
      col("audit_type").cast(IntegerType).as("audit_type"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("region_id").cast(IntegerType).as("region_id"),
      col("is_delivered").cast(IntegerType).as("is_delivered"),
      col("gdpr_consent_cookie").cast(StringType).as("gdpr_consent_cookie"),
      col("two_phase_reduction_applied")
        .cast(ByteType)
        .as("two_phase_reduction_applied"),
      col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("age").cast(IntegerType).as("age"),
      col("browser_code_id").cast(IntegerType).as("browser_code_id"),
      col("imp_ignored").cast(ByteType).as("imp_ignored"),
      col("longitude").cast(StringType).as("longitude"),
      col("clear_fees").cast(DoubleType).as("clear_fees"),
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
      col("truncate_ip").cast(IntegerType).as("truncate_ip"),
      col("dma").cast(IntegerType).as("dma"),
      col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
      col("uid_source").cast(IntegerType).as("uid_source"),
      col("seller_revenue_seller_currency")
        .cast(DoubleType)
        .as("seller_revenue_seller_currency"),
      col("preempt_ip_address").cast(StringType).as("preempt_ip_address"),
      col("buyer_currency").cast(StringType).as("buyer_currency"),
      col("imp_biddable").cast(ByteType).as("imp_biddable"),
      col("buyer_spend").cast(DoubleType).as("buyer_spend"),
      col("ss_native_assembly_enabled")
        .cast(ByteType)
        .as("ss_native_assembly_enabled"),
      col("buyer_spend_buyer_currency")
        .cast(DoubleType)
        .as("buyer_spend_buyer_currency"),
      col("language").cast(IntegerType).as("language"),
      col("seller_exchange_rate").cast(DoubleType).as("seller_exchange_rate"),
      col("site_domain").cast(StringType).as("site_domain"),
      col("view_measurable").cast(IntegerType).as("view_measurable"),
      personal_data(context).as("personal_data"),
      col("ip_address").cast(StringType).as("ip_address"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("date_time").cast(LongType).as("date_time"),
      col("device_unique_id").cast(StringType).as("device_unique_id"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("carrier_id").cast(IntegerType).as("carrier_id"),
      col("primary_width").cast(IntegerType).as("primary_width"),
      col("viewable").cast(IntegerType).as("viewable"),
      col("openrtb_req_subdomain").cast(StringType).as("openrtb_req_subdomain"),
      col("imp_rejecter_do_auction")
        .cast(ByteType)
        .as("imp_rejecter_do_auction"),
      col("auction_service_deduction")
        .cast(DoubleType)
        .as("auction_service_deduction"),
      col("external_bidrequest_id").cast(LongType).as("external_bidrequest_id"),
      col("is_prebid").cast(ByteType).as("is_prebid"),
      col("seller_deduction").cast(DoubleType).as("seller_deduction"),
      col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
      col("inventory_source_id").cast(IntegerType).as("inventory_source_id"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("is_unit_of_buyer_trx").cast(ByteType).as("is_unit_of_buyer_trx"),
      col("cookie_age").cast(IntegerType).as("cookie_age"),
      col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
      col("sdk_version").cast(StringType).as("sdk_version"),
      col("video_context").cast(IntegerType).as("video_context"),
      col("seller_currency").cast(StringType).as("seller_currency"),
      col("is_exclusive").cast(IntegerType).as("is_exclusive"),
      col("cleared_direct").cast(IntegerType).as("cleared_direct"),
      col("imp_blacklist_or_fraud")
        .cast(IntegerType)
        .as("imp_blacklist_or_fraud"),
      col("sampling_pct").cast(DoubleType).as("sampling_pct"),
      col("datacenter_id").cast(IntegerType).as("datacenter_id"),
      col("auction_service_fees").cast(DoubleType).as("auction_service_fees"),
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
      col("view_result").cast(IntegerType).as("view_result"),
      col("is_imp_rejecter_applied")
        .cast(ByteType)
        .as("is_imp_rejecter_applied"),
      col("media_type").cast(IntegerType).as("media_type"),
      col("buyer_bid_bucket").cast(IntegerType).as("buyer_bid_bucket"),
      col("pricing_media_type").cast(IntegerType).as("pricing_media_type"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      col("bidder_seat_id").cast(IntegerType).as("bidder_seat_id"),
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
      col("instance_id").cast(IntegerType).as("instance_id"),
      col("deal_type").cast(IntegerType).as("deal_type"),
      col("curated_deal_id").cast(IntegerType).as("curated_deal_id"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      col("from_imps_seen").cast(ByteType).as("from_imps_seen"),
      col("ecp").cast(DoubleType).as("ecp"),
      col("application_id").cast(StringType).as("application_id"),
      col("is_unit_of_seller_trx").cast(ByteType).as("is_unit_of_seller_trx"),
      col("user_tz_offset").cast(IntegerType).as("user_tz_offset"),
      col("imp_date_time").cast(LongType).as("imp_date_time"),
      col("default_referrer_url").cast(StringType).as("default_referrer_url"),
      col("operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("gender").cast(StringType).as("gender")
    )

  def personal_data(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("personal_data.user_id_64").as("user_id_64"),
      col("personal_data.device_unique_id").as("device_unique_id"),
      col("personal_data.external_uid").as("external_uid"),
      col("personal_data.ip_address").as("ip_address"),
      struct(col("personal_data.crossdevice_group.graph_id").as("graph_id"),
             col("personal_data.crossdevice_group.group_id").as("group_id")
      ).as("crossdevice_group"),
      col("personal_data.latitude").as("latitude"),
      col("personal_data.longitude").as("longitude"),
      col("personal_data.ipv6_address").as("ipv6_address"),
      col("personal_data.subject_to_gdpr").as("subject_to_gdpr"),
      col("personal_data.geo_country").as("geo_country"),
      col("personal_data.gdpr_consent_string").as("gdpr_consent_string"),
      col("personal_data.preempt_ip_address").as("preempt_ip_address"),
      col("personal_data.device_type").as("device_type"),
      col("personal_data.device_make_id").as("device_make_id"),
      col("personal_data.device_model_id").as("device_model_id"),
      col("personal_data.new_user_id_64").as("new_user_id_64"),
      col("personal_data.is_service_provider_mode").as(
        "is_service_provider_mode"
      ),
      col("personal_data.is_personal_info_sale").as("is_personal_info_sale")
    ).cast(
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
  }

}
