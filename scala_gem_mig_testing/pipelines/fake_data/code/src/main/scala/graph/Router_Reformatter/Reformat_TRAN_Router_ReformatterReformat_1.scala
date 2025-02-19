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

object Reformat_TRAN_Router_ReformatterReformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      coalesce(
        when(
          (coalesce(col("log_impbus_impressions.ttl").cast(IntegerType), lit(0))
            .cast(IntegerType) > lit(3600)).and(
            is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
          ),
          col("log_impbus_preempt.date_time").cast(LongType)
        ),
        col("date_time").cast(LongType)
      ).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(
          col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
        ),
        col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
        col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
      ).as("buyer_member_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.width").cast(IntegerType)),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).cast(IntegerType).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.height").cast(IntegerType)),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).cast(IntegerType).as("height"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
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
      when(col("is_dw").cast(IntegerType) > lit(-1),
           col("is_dw").cast(IntegerType)
      ).as("is_dw"),
      coalesce(
        col("log_impbus_preempt.bidder_id").cast(IntegerType),
        when(is_not_null(col("log_impbus_preempt")), lit(0)).cast(IntegerType),
        col("log_impbus_impressions.bidder_id").cast(IntegerType)
      ).as("bidder_id"),
      lit(null).cast(DoubleType).as("sampling_pct"),
      seller_revenue(context).as("seller_revenue"),
      (when(
        is_not_null(col("buyer_member_id").cast(IntegerType))
          .and(is_not_null(col("seller_member_id").cast(IntegerType)))
          .and(
            col("buyer_member_id").cast(IntegerType) =!= col("seller_member_id")
              .cast(IntegerType)
          )
          .and(
            col("log_dw_bid.payment_type").isNull.or(
              is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1)
                )
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2)
                )
            )
          ),
        coalesce(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("gross_payment_value_microcents"),
          lit(0)
        )
      ).otherwise(lit(0)).cast(DoubleType) / lit(100000))
        .cast(DoubleType)
        .as("buyer_spend"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.cleared_direct").cast(IntegerType)),
        col("log_impbus_impressions.cleared_direct").cast(IntegerType),
        col("log_impbus_preempt.cleared_direct").cast(IntegerType)
      ).cast(IntegerType).as("cleared_direct"),
      creative_overage_fees(context).as("creative_overage_fees"),
      auction_service_fees(context).as("auction_service_fees"),
      lit(null).cast(DoubleType).as("clear_fees"),
      discrepancy_allowance(context).as("discrepancy_allowance"),
      lit(null).cast(DoubleType).as("forex_allowance"),
      auction_service_deduction(context).as("auction_service_deduction"),
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
      coalesce(
        when(string_compare(col("log_impbus_preempt.buyer_currency"),
                            lit("USD")
             ) =!= lit(0),
             col("log_impbus_preempt.buyer_currency")
        ),
        when(
          col("log_impbus_preempt.buyer_currency").isNull.and(
            string_compare(col("log_impbus_impressions.buyer_currency"),
                           lit("USD")
            ) =!= lit(0)
          ),
          col("log_impbus_impressions.buyer_currency")
        )
      ).as("buyer_currency"),
      buyer_spend_buyer_currency(context).as("buyer_spend_buyer_currency"),
      when(string_compare(col("log_impbus_impressions.seller_currency"),
                          lit("USD")
           ) =!= lit(0),
           col("log_impbus_impressions.seller_currency")
      ).as("seller_currency"),
      seller_revenue_seller_currency(context)
        .as("seller_revenue_seller_currency"),
      coalesce(
        when(
          col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType) =!= lit(1),
          col("log_impbus_preempt.vp_expose_pubs").cast(IntegerType)
        ).cast(IntegerType),
        when(
          col("log_impbus_preempt.vp_expose_pubs").isNull.and(
            col("log_impbus_impressions.vp_expose_pubs")
              .cast(IntegerType) =!= lit(1)
          ),
          col("log_impbus_impressions.vp_expose_pubs").cast(IntegerType)
        ).cast(IntegerType)
      ).as("vp_expose_pubs"),
      coalesce(
        when(
          f_should_zero_seller_revenue(
            col("log_dw_bid"),
            col("imp_type").cast(IntegerType),
            col("log_dw_bid.revenue_type").cast(IntegerType),
            col("seller_member_id").cast(IntegerType),
            col(
              "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
            ).cast(LongType)
          ).cast(BooleanType),
          lit(0)
        ).cast(DoubleType),
        math_max(seller_deduction_term_id_1() + seller_deduction_term_id_74(),
                 lit(0)
        ).cast(DoubleType)
      ).as("seller_deduction"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).cast(IntegerType).as("supply_type"),
      lit(1).as("is_delivered"),
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
      coalesce(col("log_impbus_impressions.ip_address"), lit("---"))
        .as("ip_address"),
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
      f_view_measurable(
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ).cast(IntegerType),
        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType).as("view_measurable"),
      f_viewable(
        f_view_measurable(
          f_view_detection_enabled(
            col("log_impbus_impressions.view_detection_enabled").cast(
              IntegerType
            ),
            col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
          ).cast(IntegerType),
          col("log_impbus_view.view_result").cast(IntegerType)
        ).cast(IntegerType),
        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType).as("viewable"),
      coalesce(when(string_compare(col("log_impbus_impressions.call_type"),
                                   lit("")
                    ) =!= lit(0),
                    col("log_impbus_impressions.call_type")
               ),
               lit("/openrtb2")
      ).as("call_type"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.deal_id").cast(IntegerType)),
        col("log_impbus_impressions.deal_id").cast(IntegerType),
        col("log_impbus_preempt.deal_id").cast(IntegerType)
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
      coalesce(
        when(
          (coalesce(col("log_impbus_impressions.ttl").cast(IntegerType), lit(0))
            .cast(IntegerType) > lit(3600)).and(
            is_not_null(col("log_impbus_preempt.date_time").cast(LongType))
          ),
          col("log_impbus_preempt.date_time").cast(LongType)
        ).cast(LongType),
        col("date_time").cast(LongType)
      ).as("imp_date_time"),
      coalesce(
        f_preempt_over_impression_non_zero_explicit(
          is_not_null(col("log_impbus_preempt.media_type").cast(IntegerType)),
          col("log_impbus_impressions.media_type").cast(IntegerType),
          col("log_impbus_preempt.media_type").cast(IntegerType)
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
      buyer_charges(context).as("buyer_charges"),
      seller_charges(context).as("seller_charges"),
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
      when(string_compare(col("log_impbus_impressions.ip_address"),
                          col("log_impbus_preempt.ip_address")
           ) =!= lit(0),
           col("log_impbus_preempt.ip_address")
      ).as("preempt_ip_address"),
      coalesce(
        when(is_not_null(col("log_impbus_preempt.seller_transaction_def")),
             col("log_impbus_preempt.seller_transaction_def")
        ),
        when(is_not_null(col("log_impbus_impressions.seller_transaction_def")),
             col("log_impbus_impressions.seller_transaction_def")
        ),
        lit(null).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
      ).as("seller_transaction_def"),
      coalesce(
        when(is_not_null(col("log_impbus_preempt.buyer_transaction_def")),
             col("log_impbus_preempt.buyer_transaction_def")
        ),
        when(is_not_null(col("log_impbus_impressions.buyer_transaction_def")),
             col("log_impbus_impressions.buyer_transaction_def")
        ),
        lit(null).cast(
          StructType(
            Array(StructField("transaction_event",         IntegerType, true),
                  StructField("transaction_event_type_id", IntegerType, true)
            )
          )
        )
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
          col("log_impbus_impressions.audit_type").cast(IntegerType) =!= lit(0),
          col("log_impbus_impressions.audit_type").cast(IntegerType)
        ).cast(IntegerType),
        when(col("log_impbus_impressions.audit_type").isNull, lit(2))
          .cast(IntegerType)
      ).as("audit_type"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).cast(IntegerType).as("browser"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).cast(IntegerType).as("device_type"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("geo_region"),
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
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(
          col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
        ),
        col("log_impbus_impressions.vp_expose_domains").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_domains").cast(IntegerType)
      ).cast(IntegerType).as("vp_expose_domains"),
      lit(null).cast(IntegerType).as("visibility_profile_id"),
      when(
        col("log_impbus_impressions.is_exclusive").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.is_exclusive").cast(IntegerType)
      ).cast(IntegerType).as("is_exclusive"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).cast(IntegerType).as("truncate_ip"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType)),
        col("log_impbus_impressions.creative_id").cast(IntegerType),
        col("log_impbus_preempt.creative_id").cast(IntegerType)
      ).cast(IntegerType).as("creative_id"),
      coalesce(col("log_impbus_preempt.buyer_exchange_rate"),
               col("log_impbus_impressions.buyer_exchange_rate"),
               lit(0)
      ).cast(DoubleType).as("buyer_exchange_rate"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)),
        col("log_impbus_impressions.vp_expose_tag").cast(IntegerType),
        col("log_impbus_preempt.vp_expose_tag").cast(IntegerType)
      ).cast(IntegerType).as("vp_expose_tag"),
      f_view_result(
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ).cast(IntegerType),
        col("log_impbus_view.view_result").cast(IntegerType)
      ).cast(IntegerType).as("view_result"),
      when(col("log_impbus_impressions.age").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.age").cast(IntegerType)
      ).cast(IntegerType).as("age"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.brand_id").cast(IntegerType)),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).cast(IntegerType).as("brand_id"),
      when(
        col("log_impbus_impressions.carrier_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.carrier_id").cast(IntegerType)
      ).cast(IntegerType).as("carrier_id"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).cast(IntegerType).as("city"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).cast(IntegerType).as("dma"),
      when(string_compare(col("log_impbus_impressions.device_unique_id"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.device_unique_id")
      ).as("device_unique_id"),
      when(string_compare(col("log_impbus_impressions.latitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.latitude")
      ).as("latitude"),
      when(string_compare(col("log_impbus_impressions.longitude"),
                          lit("")
           ) =!= lit(0),
           col("log_impbus_impressions.longitude")
      ).as("longitude"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal"),
      col("log_impbus_impressions.sdk_version").as("sdk_version"),
      coalesce(col("log_impbus_impressions_pricing.rate_card_media_type").cast(
                 IntegerType
               ),
               lit(0).cast(IntegerType)
      ).as("pricing_media_type"),
      col("log_impbus_impressions.traffic_source_code")
        .as("traffic_source_code"),
      when(col("log_impbus_impressions.is_prebid").cast(ByteType) =!= lit(0),
           col("log_impbus_impressions.is_prebid").cast(ByteType)
      ).cast(ByteType).as("is_prebid"),
      is_unit_of_buyer_trx(context).as("is_unit_of_buyer_trx"),
      is_unit_of_seller_trx(context).as("is_unit_of_seller_trx"),
      col("log_impbus_impressions_pricing.two_phase_reduction_applied")
        .cast(ByteType)
        .as("two_phase_reduction_applied"),
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
      coalesce(col("log_impbus_preempt.external_creative_id"),
               when(is_not_null(col("log_impbus_preempt")), lit("---"))
      ).as("external_creative_id"),
      when(
        col("log_impbus_impressions.subject_to_gdpr").cast(ByteType) =!= lit(0),
        col("log_impbus_impressions.subject_to_gdpr").cast(ByteType)
      ).cast(ByteType).as("subject_to_gdpr"),
      when(col("log_impbus_impressions.fx_rate_snapshot_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.fx_rate_snapshot_id").cast(IntegerType)
      ).cast(IntegerType).as("fx_rate_snapshot_id"),
      when(
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ).cast(IntegerType) =!= lit(0),
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        ).cast(IntegerType)
      ).cast(IntegerType).as("view_detection_enabled"),
      coalesce(col("log_impbus_impressions.seller_exchange_rate"), lit(0))
        .cast(DoubleType)
        .as("seller_exchange_rate"),
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
        lit(0).cast(IntegerType)
      ).as("bidder_seat_id"),
      lit(null).cast(StringType).as("default_referrer_url"),
      when(col("log_impbus_impressions.pred_info").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.pred_info").cast(IntegerType)
      ).cast(IntegerType).as("pred_info"),
      when(
        !(coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                   lit(0)
        ) =!= lit(0)),
        lit(null).cast(IntegerType)
      ).otherwise(
          coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                   lit(0)
          ).cast(IntegerType)
        )
        .cast(IntegerType)
        .as("curated_deal_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.deal_type").cast(IntegerType)),
        col("log_impbus_impressions.deal_type").cast(IntegerType),
        col("log_impbus_preempt.deal_type").cast(IntegerType)
      ).cast(IntegerType).as("deal_type"),
      coalesce(
        when(size(col("log_impbus_impressions.tag_sizes")) > lit(0),
             element_at(col("log_impbus_impressions.tag_sizes"), 1).getField(
               "height"
             )
        ).cast(IntegerType),
        f_preempt_over_impression_non_zero_explicit(
          is_not_null(col("log_impbus_preempt.height").cast(IntegerType)),
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
        f_preempt_over_impression_non_zero_explicit(
          is_not_null(col("log_impbus_preempt.width").cast(IntegerType)),
          col("log_impbus_impressions.width").cast(IntegerType),
          col("log_impbus_preempt.width").cast(IntegerType)
        ).cast(IntegerType),
        lit(300).cast(IntegerType)
      ).as("primary_width"),
      when(
        coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                 lit(0)
        ) =!= lit(0),
        coalesce(
          lookup("sup_common_deal",
                 col("log_impbus_preempt.curated_deal_id").cast(IntegerType)
          ).getField("member_id"),
          lit(0)
        )
      ).otherwise(lit(null).cast(IntegerType))
        .cast(IntegerType)
        .as("curator_member_id"),
      lit(null).cast(IntegerType).as("instance_id"),
      when(col("log_impbus_impressions.hb_source").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.hb_source").cast(IntegerType)
      ).cast(IntegerType).as("hb_source"),
      lit(null).cast(ByteType).as("from_imps_seen"),
      f_preempt_over_impression(
        col("log_impbus_impressions.external_campaign_id"),
        col("log_impbus_preempt.external_campaign_id")
      ).as("external_campaign_id"),
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
      coalesce(col("log_impbus_preempt.external_bidrequest_id").cast(LongType),
               lit(0)
      ).cast(LongType).as("external_bidrequest_id"),
      coalesce(
        col("log_impbus_preempt.external_bidrequest_imp_id").cast(LongType),
        lit(0)
      ).cast(LongType).as("external_bidrequest_imp_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_floor_id").cast(IntegerType),
        col("log_impbus_preempt.ym_floor_id").cast(IntegerType)
      ).cast(IntegerType).as("ym_floor_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.ym_bias_id").cast(IntegerType),
        col("log_impbus_preempt.ym_bias_id").cast(IntegerType)
      ).cast(IntegerType).as("ym_bias_id"),
      coalesce(
        when(is_not_null(col("log_impbus_impressions.openrtb_req_subdomain")),
             col("log_impbus_impressions.openrtb_req_subdomain")
        ),
        lit(null).cast(StringType)
      ).as("openrtb_req_subdomain")
    )

  def buyer_spend_buyer_currency(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    ((when(
      is_not_null(col("buyer_member_id").cast(IntegerType))
        .and(is_not_null(col("seller_member_id").cast(IntegerType)))
        .and(
          col("buyer_member_id").cast(IntegerType) =!= col("seller_member_id")
            .cast(IntegerType)
        )
        .and(
          col("log_dw_bid.payment_type").isNull.or(
            is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
              .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1))
              .and(col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2))
          )
        ),
      coalesce(
        f_get_transaction_event_pricing(
          col("log_impbus_impressions_pricing.impression_event_pricing"),
          col("log_impbus_auction_event.auction_event_pricing"),
          col("log_impbus_impressions_pricing.buyer_charges"),
          col("log_impbus_impressions_pricing.seller_charges"),
          f_should_process_views(
            col("log_dw_view"),
            f_transaction_event(
              col("log_impbus_impressions.seller_transaction_def"),
              col("log_impbus_preempt.seller_transaction_def")
            ).cast(IntegerType),
            f_transaction_event(
              col("log_impbus_impressions.buyer_transaction_def"),
              col("log_impbus_preempt.buyer_transaction_def")
            ).cast(IntegerType)
          ).cast(IntegerType)
        ).getField("gross_payment_value_microcents"),
        lit(0)
      )
    ).otherwise(lit(0)).cast(DoubleType) / lit(100000))
      .cast(DoubleType) * coalesce(
      col("log_impbus_preempt.buyer_exchange_rate"),
      col("log_impbus_impressions.buyer_exchange_rate"),
      lit(0)
    ).cast(DoubleType)).cast(DoubleType)
  }

  def auction_service_deduction(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount")
      ).and(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") > lit(0)
      ),
      when(
        is_not_null(
          f_get_pricing_term(
            lit(1),
            when(
              is_not_null(
                f_get_impression_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges")
                ).getField("buyer_charges").getField("pricing_terms")
              ),
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
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
                )
              )
            )
          ).getField("is_deduction")
        ).and(
          f_get_pricing_term(
            lit(1),
            when(
              is_not_null(
                f_get_impression_transaction_event_pricing(
                  col(
                    "log_impbus_impressions_pricing.impression_event_pricing"
                  ),
                  col("log_impbus_impressions_pricing.buyer_charges"),
                  col("log_impbus_impressions_pricing.seller_charges")
                ).getField("buyer_charges").getField("pricing_terms")
              ),
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ).otherwise(
              lit(null).cast(
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
                )
              )
            )
          ).getField("is_deduction") === lit(1)
        ),
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") / lit(1000)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(DoubleType)
  }

  def seller_revenue(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    coalesce(
      when(
        f_should_zero_seller_revenue(
          col("log_dw_bid"),
          col("imp_type").cast(IntegerType),
          col("log_dw_bid.revenue_type").cast(IntegerType),
          col("seller_member_id").cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
          ).cast(LongType)
        ).cast(BooleanType),
        lit(0)
      ).cast(DoubleType),
      (when(
        is_not_null(col("buyer_member_id").cast(IntegerType))
          .and(is_not_null(col("seller_member_id").cast(IntegerType)))
          .and(
            col("buyer_member_id").cast(IntegerType) =!= col("seller_member_id")
              .cast(IntegerType)
          )
          .and(
            col("log_dw_bid.payment_type").isNull.or(
              is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1)
                )
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2)
                )
            )
          ),
        coalesce(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("seller_revenue_microcents"),
          lit(0)
        )
      ).otherwise(lit(0)).cast(DoubleType) / lit(100000)).cast(DoubleType)
    )
  }

  def creative_overage_fees(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(
        f_get_pricing_term(
          lit(11),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount")
      ).and(
        f_get_pricing_term(
          lit(11),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") > lit(0)
      ),
      f_get_pricing_term(
        lit(11),
        when(
          is_not_null(
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ),
          f_get_impression_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges")
          ).getField("buyer_charges").getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
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
            )
          )
        )
      ).getField("amount") / lit(1000)
    ).otherwise(lit(0)).cast(DoubleType)
  }

  def buyer_charges(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.rate_card_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.rate_card_id").cast(
          IntegerType
        )
      ).as("rate_card_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.member_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.member_id").cast(
          IntegerType
        )
      ).as("member_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.is_dw"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(ByteType)
      ).as("is_dw"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.fx_margin_rate_id")
          .cast(IntegerType)
      ).as("fx_margin_rate_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.buyer_charges.marketplace_owner_id")
          .cast(IntegerType)
      ).as("marketplace_owner_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.buyer_charges.virtual_marketplace_id"
        ).cast(IntegerType)
      ).as("virtual_marketplace_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.amino_enabled"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.amino_enabled"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.buyer_charges.amino_enabled").cast(
          ByteType
        )
      ).as("amino_enabled"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.buyer_charges.pricing_terms"
        ),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.buyer_charges.pricing_terms"
        ),
        buyer_charges_pricing_terms()
      ).as("pricing_terms")
    )
  }

  def is_unit_of_buyer_trx(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    !is_not_null(col("log_impbus_preempt.buyer_transaction_def"))
      .or(is_not_null(col("log_impbus_impressions.buyer_transaction_def")))
      .or(
        when(
          f_has_transacted(col("log_impbus_impressions.buyer_transaction_def"),
                           col("log_impbus_preempt.buyer_transaction_def")
          ) === lit(0),
          coalesce(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ).cast(IntegerType),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                ).cast(IntegerType)
              ).cast(IntegerType)
            ).getField("buyer_transacted"),
            lit(0)
          )
        ).otherwise(
            f_has_transacted(
              col("log_impbus_impressions.buyer_transaction_def"),
              col("log_impbus_preempt.buyer_transaction_def")
            ).cast(ByteType)
          )
          .cast(BooleanType)
      )
      .or(
        is_not_null(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("buyer_transacted")
        ).and(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("buyer_transacted") === lit(1)
        )
      )
      .cast(ByteType)
  }

  def is_unit_of_seller_trx(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    !is_not_null(col("log_impbus_preempt.seller_transaction_def"))
      .or(is_not_null(col("log_impbus_impressions.seller_transaction_def")))
      .or(
        when(
          f_has_transacted(col("log_impbus_impressions.seller_transaction_def"),
                           col("log_impbus_preempt.seller_transaction_def")
          ) === lit(0),
          coalesce(
            f_get_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_auction_event.auction_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges"),
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ).cast(IntegerType),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                ).cast(IntegerType)
              ).cast(IntegerType)
            ).getField("seller_transacted"),
            lit(0)
          )
        ).otherwise(
            f_has_transacted(
              col("log_impbus_impressions.seller_transaction_def"),
              col("log_impbus_preempt.seller_transaction_def")
            ).cast(ByteType)
          )
          .cast(BooleanType)
      )
      .or(
        is_not_null(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("seller_transacted")
        ).and(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("seller_transacted") === lit(1)
        )
      )
      .cast(ByteType)
  }

  def seller_revenue_seller_currency(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    coalesce(
      when(
        f_should_zero_seller_revenue(
          col("log_dw_bid"),
          col("imp_type").cast(IntegerType),
          col("log_dw_bid.revenue_type").cast(IntegerType),
          col("seller_member_id").cast(IntegerType),
          col(
            "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
          ).cast(LongType)
        ).cast(BooleanType),
        lit(0)
      ).cast(DoubleType),
      ((when(
        is_not_null(col("buyer_member_id").cast(IntegerType))
          .and(is_not_null(col("seller_member_id").cast(IntegerType)))
          .and(
            col("buyer_member_id").cast(IntegerType) =!= col("seller_member_id")
              .cast(IntegerType)
          )
          .and(
            col("log_dw_bid.payment_type").isNull.or(
              is_not_null(col("log_dw_bid.payment_type").cast(IntegerType))
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(1)
                )
                .and(
                  col("log_dw_bid.payment_type").cast(IntegerType) =!= lit(2)
                )
            )
          ),
        coalesce(
          f_get_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_auction_event.auction_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges"),
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ).cast(IntegerType),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              ).cast(IntegerType)
            ).cast(IntegerType)
          ).getField("seller_revenue_microcents"),
          lit(0)
        )
      ).otherwise(lit(0)).cast(DoubleType) / lit(100000))
        .cast(DoubleType) * coalesce(
        col("log_impbus_impressions.seller_exchange_rate"),
        lit(0)
      ).cast(DoubleType)).cast(DoubleType)
    )
  }

  def seller_charges(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.rate_card_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.rate_card_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.rate_card_id").cast(
          IntegerType
        )
      ).as("rate_card_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.member_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.member_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.member_id").cast(
          IntegerType
        )
      ).as("member_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.is_dw"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.is_dw"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
          ByteType
        )
      ).as("is_dw"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.fx_margin_rate_id"
        ).cast(IntegerType),
        col("log_impbus_impressions_pricing.seller_charges.fx_margin_rate_id")
          .cast(IntegerType)
      ).as("fx_margin_rate_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.seller_charges.marketplace_owner_id"
        ).cast(IntegerType)
      ).as("marketplace_owner_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType),
        col(
          "log_impbus_impressions_pricing.seller_charges.virtual_marketplace_id"
        ).cast(IntegerType)
      ).as("virtual_marketplace_id"),
      coalesce(
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.amino_enabled"
        ).cast(ByteType),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.amino_enabled"
        ).cast(ByteType),
        col("log_impbus_impressions_pricing.seller_charges.amino_enabled").cast(
          ByteType
        )
      ).as("amino_enabled"),
      coalesce(
        when(
          f_should_zero_seller_revenue(
            col("log_dw_bid"),
            col("imp_type").cast(IntegerType),
            col("log_dw_bid.revenue_type").cast(IntegerType),
            col("seller_member_id").cast(IntegerType),
            col(
              "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
            ).cast(LongType)
          ).cast(BooleanType),
          f_drop_is_deduction_pricing_terms(seller_charges_pricing_terms())
        ),
        col(
          "log_impbus_auction_event.auction_event_pricing.seller_charges.pricing_terms"
        ),
        col(
          "log_impbus_impressions_pricing.impression_event_pricing.seller_charges.pricing_terms"
        ),
        seller_charges_pricing_terms()
      ).as("pricing_terms")
    )
  }

  def auction_service_fees(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount")
      ).and(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") > lit(0)
      ),
      when(
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("is_deduction")
          .isNull
          .or(
            is_not_null(
              f_get_pricing_term(
                lit(1),
                when(
                  is_not_null(
                    f_get_impression_transaction_event_pricing(
                      col(
                        "log_impbus_impressions_pricing.impression_event_pricing"
                      ),
                      col("log_impbus_impressions_pricing.buyer_charges"),
                      col("log_impbus_impressions_pricing.seller_charges")
                    ).getField("buyer_charges").getField("pricing_terms")
                  ),
                  f_get_impression_transaction_event_pricing(
                    col(
                      "log_impbus_impressions_pricing.impression_event_pricing"
                    ),
                    col("log_impbus_impressions_pricing.buyer_charges"),
                    col("log_impbus_impressions_pricing.seller_charges")
                  ).getField("buyer_charges").getField("pricing_terms")
                ).otherwise(
                  lit(null).cast(
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
                    )
                  )
                )
              ).getField("is_deduction")
            ).and(
              f_get_pricing_term(
                lit(1),
                when(
                  is_not_null(
                    f_get_impression_transaction_event_pricing(
                      col(
                        "log_impbus_impressions_pricing.impression_event_pricing"
                      ),
                      col("log_impbus_impressions_pricing.buyer_charges"),
                      col("log_impbus_impressions_pricing.seller_charges")
                    ).getField("buyer_charges").getField("pricing_terms")
                  ),
                  f_get_impression_transaction_event_pricing(
                    col(
                      "log_impbus_impressions_pricing.impression_event_pricing"
                    ),
                    col("log_impbus_impressions_pricing.buyer_charges"),
                    col("log_impbus_impressions_pricing.seller_charges")
                  ).getField("buyer_charges").getField("pricing_terms")
                ).otherwise(
                  lit(null).cast(
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
                    )
                  )
                )
              ).getField("is_deduction") === lit(0)
            )
          ),
        f_get_pricing_term(
          lit(1),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") / lit(1000)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(DoubleType)
  }

  def discrepancy_allowance(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(
        f_get_pricing_term(
          lit(51),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount")
      ).and(
        f_get_pricing_term(
          lit(51),
          when(
            is_not_null(
              f_get_impression_transaction_event_pricing(
                col("log_impbus_impressions_pricing.impression_event_pricing"),
                col("log_impbus_impressions_pricing.buyer_charges"),
                col("log_impbus_impressions_pricing.seller_charges")
              ).getField("buyer_charges").getField("pricing_terms")
            ),
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ).otherwise(
            lit(null).cast(
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
              )
            )
          )
        ).getField("amount") > lit(0)
      ),
      f_get_pricing_term(
        lit(51),
        when(
          is_not_null(
            f_get_impression_transaction_event_pricing(
              col("log_impbus_impressions_pricing.impression_event_pricing"),
              col("log_impbus_impressions_pricing.buyer_charges"),
              col("log_impbus_impressions_pricing.seller_charges")
            ).getField("buyer_charges").getField("pricing_terms")
          ),
          f_get_impression_transaction_event_pricing(
            col("log_impbus_impressions_pricing.impression_event_pricing"),
            col("log_impbus_impressions_pricing.buyer_charges"),
            col("log_impbus_impressions_pricing.seller_charges")
          ).getField("buyer_charges").getField("pricing_terms")
        ).otherwise(
          lit(null).cast(
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
            )
          )
        )
      ).getField("amount") / lit(1000)
    ).otherwise(lit(0)).cast(DoubleType)
  }

}
