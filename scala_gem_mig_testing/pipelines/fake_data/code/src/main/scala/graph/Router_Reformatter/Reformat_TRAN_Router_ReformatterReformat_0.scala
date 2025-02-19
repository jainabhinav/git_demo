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

object Reformat_TRAN_Router_ReformatterReformat_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      when(col("log_impbus_impressions.date_time").cast(LongType) =!= lit(0),
           col("log_impbus_impressions.date_time").cast(LongType)
      ).as("date_time"),
      when(
        col("log_impbus_impressions.auction_id_64").cast(LongType) =!= lit(0),
        col("log_impbus_impressions.auction_id_64").cast(LongType)
      ).as("auction_id_64"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.buyer_member_id").cast(IntegerType),
        col("log_impbus_preempt.buyer_member_id").cast(IntegerType)
      ).as("buyer_member_id"),
      when(col("log_impbus_impressions.seller_member_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.seller_member_id").cast(IntegerType)
      ).as("seller_member_id"),
      when(
        coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                 lit(0)
        ).cast(IntegerType) =!= lit(0),
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
        ).otherwise(lit(0)).cast(IntegerType)
      ).otherwise(lit(0)).cast(IntegerType).as("curator_member_id"),
      when(
        coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                 lit(0)
        ).cast(IntegerType) =!= lit(0),
        col("log_dw_bid_curator.member_id").cast(IntegerType)
      ).as("member_id"),
      when(col("log_dw_bid_curator.advertiser_id").cast(IntegerType) =!= lit(0),
           col("log_dw_bid_curator.advertiser_id").cast(IntegerType)
      ).as("advertiser_id"),
      when(
        col("log_impbus_impressions.publisher_id").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.publisher_id").cast(IntegerType)
      ).as("publisher_id"),
      when(col("log_impbus_preempt.bidder_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_preempt.bidder_id").cast(IntegerType)
      ).cast(IntegerType).as("bidder_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.width").cast(IntegerType),
        col("log_impbus_preempt.width").cast(IntegerType)
      ).cast(IntegerType).as("width"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.height").cast(IntegerType),
        col("log_impbus_preempt.height").cast(IntegerType)
      ).cast(IntegerType).as("height"),
      when(col("log_impbus_impressions.site_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.site_id").cast(IntegerType)
      ).as("site_id"),
      when(col("log_impbus_impressions.tag_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.tag_id").cast(IntegerType)
      ).as("tag_id"),
      col("log_impbus_impressions.geo_country").as("geo_country"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.brand_id").cast(IntegerType),
        col("log_impbus_preempt.brand_id").cast(IntegerType)
      ).cast(IntegerType).as("brand_id"),
      coalesce(col("log_impbus_impressions.site_domain"), lit("---"))
        .as("site_domain"),
      coalesce(col("log_impbus_impressions.application_id"), lit("---"))
        .as("application_id"),
      when(
        col("log_impbus_impressions.device_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.device_type").cast(IntegerType)
      ).cast(IntegerType).as("device_type"),
      when(col("log_dw_bid_curator.insertion_order_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_dw_bid_curator.insertion_order_id").cast(IntegerType)
      ).as("insertion_order_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.media_type").cast(IntegerType),
        col("log_impbus_preempt.media_type").cast(IntegerType)
      ).cast(IntegerType).as("media_type"),
      coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
               lit(0)
      ).cast(IntegerType).as("curated_deal_id"),
      when(coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
           ).cast(IntegerType) =!= lit(0),
           lit(5)
      ).cast(IntegerType).as("curated_deal_type"),
      coalesce(
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ).cast(IntegerType) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) =!= lit(0)
          ),
          f_preempt_over_impression_non_zero_explicit(
            is_not_null(col("log_impbus_preempt")),
            col("log_impbus_impressions.deal_id").cast(IntegerType),
            col("log_impbus_preempt.deal_id").cast(IntegerType)
          )
        ).cast(IntegerType),
        lit(0).cast(IntegerType)
      ).as("seller_deal_id"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        f_preempt_over_impression(
          col("log_impbus_impressions.deal_type").cast(IntegerType),
          col("log_impbus_preempt.deal_type").cast(IntegerType)
        )
      ).cast(IntegerType).as("seller_deal_type"),
      when(col("log_dw_bid_curator.campaign_group_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_dw_bid_curator.campaign_group_id").cast(IntegerType)
      ).as("campaign_group_id"),
      lit(null).cast(IntegerType).as("campaign_group_type_id"),
      when(col("log_impbus_impressions.fx_rate_snapshot_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.fx_rate_snapshot_id").cast(IntegerType)
      ).cast(IntegerType).as("fx_rate_snapshot_id"),
      when(coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
           ).cast(IntegerType) =!= lit(0),
           lit(1)
      ).cast(ByteType).as("is_curated"),
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
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ) =!= lit(0)
          ),
          f_view_result(
            f_view_detection_enabled(
              col("log_impbus_impressions.view_detection_enabled").cast(
                IntegerType
              ),
              col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
            ),
            col("log_impbus_view.view_result").cast(IntegerType)
          )
        ).otherwise(lit(0)).cast(IntegerType)
      ).cast(IntegerType).as("view_result"),
      lit(null).cast(IntegerType).as("view_detection_enabled"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ) =!= lit(0)
          ),
          f_viewdef_definition_id(
            col("log_impbus_impressions.viewdef_definition_id_buyer_member")
              .cast(IntegerType),
            col("log_impbus_preempt.viewdef_definition_id_buyer_member").cast(
              IntegerType
            ),
            col("log_impbus_view.viewdef_definition_id").cast(IntegerType)
          )
        ).otherwise(lit(0)).cast(IntegerType)
      ).cast(IntegerType).as("viewdef_definition_id"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ) =!= lit(0)
          ),
          f_viewdef_viewable(
            f_viewdef_definition_id(
              col("log_impbus_impressions.viewdef_definition_id_buyer_member")
                .cast(IntegerType),
              col("log_impbus_preempt.viewdef_definition_id_buyer_member").cast(
                IntegerType
              ),
              col("log_impbus_view.viewdef_definition_id").cast(IntegerType)
            ),
            f_view_measurable(
              f_view_detection_enabled(
                col("log_impbus_impressions.view_detection_enabled").cast(
                  IntegerType
                ),
                col("log_impbus_preempt.view_detection_enabled").cast(
                  IntegerType
                )
              ),
              col("log_impbus_view.view_result").cast(IntegerType)
            ),
            col("log_impbus_view.viewdef_view_result").cast(IntegerType)
          )
        ).otherwise(lit(0)).cast(IntegerType)
      ).cast(IntegerType).as("viewdef_viewable"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ) =!= lit(0)
          ),
          f_view_measurable(
            f_view_detection_enabled(
              col("log_impbus_impressions.view_detection_enabled").cast(
                IntegerType
              ),
              col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
            ),
            col("log_impbus_view.view_result").cast(IntegerType)
          )
        ).otherwise(lit(0)).cast(IntegerType)
      ).cast(IntegerType).as("view_measurable"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ).cast(IntegerType) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ).cast(IntegerType) =!= lit(0)
        ),
        when(
          (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                    lit(0)
          ) =!= lit(0)).and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ) =!= lit(0)
          ),
          f_viewable(
            f_view_measurable(
              f_view_detection_enabled(
                col("log_impbus_impressions.view_detection_enabled").cast(
                  IntegerType
                ),
                col("log_impbus_preempt.view_detection_enabled").cast(
                  IntegerType
                )
              ),
              col("log_impbus_view.view_result").cast(IntegerType)
            ),
            col("log_impbus_view.view_result").cast(IntegerType)
          )
        ).otherwise(lit(0)).cast(IntegerType)
      ).cast(IntegerType).as("viewable"),
      lit(0).cast(LongType).as("total_data_costs_microcents"),
      total_cost_microcents(context).as("total_cost_microcents"),
      lit(0).cast(LongType).as("total_partner_fees_microcents"),
      net_media_cost_microcents(context).as("net_media_cost_microcents"),
      gross_revenue_microcents(context).as("gross_revenue_microcents"),
      when(
        is_not_null(col("log_impbus_impressions_pricing"))
          .and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            )
          ),
        decimal_round_even(
          f_get_total_tech_fees(
            when(
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              ) === lit(1),
              col(
                "log_impbus_auction_event.auction_event_pricing.buyer_charges"
              ).getField("pricing_terms")
            ).otherwise(
              col("log_impbus_impressions_pricing.buyer_charges")
                .getField("pricing_terms")
            )
          ) * lit(100000.0d),
          0
        )
      ).otherwise(lit(0)).cast(LongType).as("total_tech_fees_microcents"),
      col("log_dw_bid_curator.targeted_segment_details")
        .as("targeted_segment_details"),
      col("log_dw_bid_curator.excluded_targeted_segment_details")
        .as("excluded_targeted_segment_details"),
      when(
        col("log_impbus_impressions.supply_type").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.supply_type").cast(IntegerType)
      ).cast(IntegerType).as("supply_type"),
      when(col("log_impbus_impressions.vp_expose_domains").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.vp_expose_domains").cast(IntegerType)
      ).cast(IntegerType).as("vp_expose_domains"),
      when(col("log_impbus_impressions.inventory_url_id").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.inventory_url_id").cast(IntegerType)
      ).cast(IntegerType).as("inventory_url_id"),
      when(
        is_not_null(col("log_impbus_impressions_pricing"))
          .and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            )
          ),
        decimal_round_even(
          f_get_curator_margin(
            when(
              f_should_process_views(
                col("log_dw_view"),
                f_transaction_event(
                  col("log_impbus_impressions.seller_transaction_def"),
                  col("log_impbus_preempt.seller_transaction_def")
                ),
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                )
              ) === lit(1),
              col(
                "log_impbus_auction_event.auction_event_pricing.buyer_charges"
              ).getField("pricing_terms")
            ).otherwise(
              col("log_impbus_impressions_pricing.buyer_charges")
                .getField("pricing_terms")
            )
          ) * lit(100000.0d),
          0
        )
      ).otherwise(lit(0)).cast(LongType).as("curator_margin_microcents"),
      when(
        is_not_null(col("log_impbus_impressions_pricing"))
          .and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            )
          ),
        f_get_is_curator_margin_media_cost_dependent(
          when(
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              )
            ) === lit(1),
            col("log_impbus_auction_event.auction_event_pricing.buyer_charges")
              .getField("pricing_terms")
          ).otherwise(
            col("log_impbus_impressions_pricing.buyer_charges")
              .getField("pricing_terms")
          )
        )
      ).otherwise(lit(0))
        .cast(ByteType)
        .as("is_curator_margin_media_cost_dependent"),
      when(
        is_not_null(col("log_impbus_impressions_pricing"))
          .and(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) === lit(1)
          )
          .and(
            is_not_null(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            )
          ),
        f_get_curator_margin_type(
          when(
            f_should_process_views(
              col("log_dw_view"),
              f_transaction_event(
                col("log_impbus_impressions.seller_transaction_def"),
                col("log_impbus_preempt.seller_transaction_def")
              ),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              )
            ) === lit(1),
            col("log_impbus_auction_event.auction_event_pricing.buyer_charges")
              .getField("pricing_terms")
          ).otherwise(
            col("log_impbus_impressions_pricing.buyer_charges")
              .getField("pricing_terms")
          )
        )
      ).otherwise(lit(0)).cast(IntegerType).as("curator_margin_type"),
      coalesce(col("log_impbus_preempt.seat_id").cast(IntegerType),
               lit(0).cast(IntegerType)
      ).as("bidder_seat_id"),
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
      when(col("log_impbus_impressions.user_tz_offset").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.user_tz_offset").cast(IntegerType)
      ).cast(IntegerType).as("user_tz_offset"),
      when(string_compare(col("log_impbus_impressions.geo_region"),
                          lit("--")
           ) =!= lit(0),
           col("log_impbus_impressions.geo_region")
      ).as("region"),
      when(col("log_impbus_impressions.dma").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.dma").cast(IntegerType)
      ).cast(IntegerType).as("dma"),
      when(col("log_impbus_impressions.city").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.city").cast(IntegerType)
      ).cast(IntegerType).as("city"),
      when(string_compare(col("log_impbus_impressions.postal"),
                          lit("---")
           ) =!= lit(0),
           col("log_impbus_impressions.postal")
      ).as("postal_code"),
      when(
        col("log_impbus_impressions.mobile_app_instance_id").cast(
          IntegerType
        ) =!= lit(0),
        col("log_impbus_impressions.mobile_app_instance_id").cast(IntegerType)
      ).cast(IntegerType).as("mobile_app_instance_id"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt.creative_id").cast(IntegerType)),
        col("log_impbus_impressions.creative_id").cast(IntegerType),
        col("log_impbus_preempt.creative_id").cast(IntegerType)
      ).cast(IntegerType).as("creative_id"),
      when(
        col("log_impbus_impressions.truncate_ip").cast(IntegerType) =!= lit(0),
        col("log_impbus_impressions.truncate_ip").cast(IntegerType)
      ).cast(IntegerType).as("truncate_ip"),
      f_preempt_over_impression_non_zero_explicit(
        is_not_null(col("log_impbus_preempt")),
        col("log_impbus_impressions.vp_bitmap").cast(LongType),
        col("log_impbus_preempt.vp_bitmap").cast(LongType)
      ).cast(LongType).as("vp_bitmap"),
      col("log_impbus_impressions.gdpr_consent_cookie")
        .as("gdpr_consent_string"),
      col("log_impbus_impressions.anonymized_user_info")
        .as("anonymized_user_info"),
      when(
        (coalesce(col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
        ) =!= lit(0)).and(
          coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                     .cast(ByteType),
                   lit(0)
          ) =!= lit(0)
        ),
        f_view_non_measurable_reason(
          f_view_detection_enabled(
            col("log_impbus_impressions.view_detection_enabled").cast(
              IntegerType
            ),
            col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
          ),
          col("log_impbus_view.view_result").cast(IntegerType)
        )
      ).otherwise(lit(0)).cast(IntegerType).as("view_non_measurable_reason"),
      when(col("log_impbus_impressions.operating_system").cast(
             IntegerType
           ) =!= lit(0),
           col("log_impbus_impressions.operating_system").cast(IntegerType)
      ).cast(IntegerType).as("operating_system"),
      when(col("log_impbus_impressions.browser").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.browser").cast(IntegerType)
      ).cast(IntegerType).as("browser"),
      when(col("log_impbus_impressions.language").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.language").cast(IntegerType)
      ).cast(IntegerType).as("language"),
      when(col("log_impbus_impressions.device_id").cast(IntegerType) =!= lit(0),
           col("log_impbus_impressions.device_id").cast(IntegerType)
      ).cast(IntegerType).as("device_id"),
      data_costs(context).as("data_costs"),
      coalesce(
        when(
          when(
            is_not_null(col("log_impbus_impressions_pricing")),
            when(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1),
              f_transaction_event(
                col("log_impbus_impressions.buyer_transaction_def"),
                col("log_impbus_preempt.buyer_transaction_def")
              )
            ).otherwise(lit(0))
          ).otherwise(lit(0)).cast(IntegerType) =!= lit(1),
          lit(null)
        ),
        col("log_dw_bid_curator.crossdevice_graph_cost")
      ).as("crossdevice_graph_cost"),
      col("log_impbus_impressions.personal_identifiers")
        .as("personal_identifiers"),
      col("log_dw_bid_curator.crossdevice_group_anon")
        .as("crossdevice_group_anon"),
      col("log_dw_bid_curator.crossdevice_graph_membership")
        .as("crossdevice_graph_membership"),
      col("log_dw_bid_curator.has_crossdevice_reach_extension")
        .cast(ByteType)
        .as("has_crossdevice_reach_extension"),
      col("log_dw_bid_curator.targeted_crossdevice_graph_id")
        .cast(IntegerType)
        .as("targeted_crossdevice_graph_id"),
      coalesce(col("log_dw_bid_curator.line_item_currency"), lit("---"))
        .as("curated_line_item_currency"),
      col("log_dw_bid_curator.split_id").cast(IntegerType).as("split_id"),
      coalesce(col("log_dw_bid_curator.bidding_host_id").cast(IntegerType),
               lit(0)
      ).cast(IntegerType).as("bidding_host_id"),
      lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
      lit(null).cast(LongType).as("seller_dpvp_bitmap"),
      f_preempt_over_impression(
        col("log_impbus_impressions.external_campaign_id"),
        col("log_impbus_preempt.external_campaign_id")
      ).as("external_campaign_id"),
      coalesce(
        col("log_impbus_preempt.external_bidrequest_imp_id").cast(LongType),
        lit(0)
      ).cast(LongType).as("external_bidrequest_imp_id"),
      coalesce(col("log_impbus_preempt.external_bidrequest_id").cast(LongType),
               lit(0)
      ).cast(LongType).as("external_bidrequest_id"),
      col("log_impbus_impressions.postal_code_ext_id")
        .cast(IntegerType)
        .as("postal_code_ext_id")
    )

  def net_media_cost_microcents(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_impressions_pricing")),
      when(
        coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                   .cast(ByteType),
                 lit(0)
        ).cast(IntegerType) === lit(1),
        coalesce(col(
                   "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
                 ).cast(LongType),
                 lit(0)
        ).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                col(
                  "log_impbus_impressions_pricing.seller_charges.pricing_terms"
                )
              )
            ),
          decimal_round_even(
            f_get_seller_fees(
              col("log_impbus_impressions_pricing.seller_charges.pricing_terms")
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing")),
          when(
            (coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                        .cast(ByteType),
                      lit(0)
            ).cast(IntegerType) === lit(1))
              .and(
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                ) === lit(1)
              )
              .and(is_not_null(col("log_dw_bid_deal.data_costs"))),
            aggregate(
              f_update_data_costs_deal(
                col("log_dw_bid_deal.data_costs"),
                coalesce(
                  lookup(
                    "sup_bidder_member_sales_tax_rate",
                    coalesce(
                      lookup("sup_common_deal",
                             col("log_impbus_preempt.deal_id").cast(IntegerType)
                      ).getField("member_id"),
                      lit(0)
                    )
                  ).getField("sales_tax_rate_pct"),
                  lit(0)
                )
              ),
              lit(0).cast(DoubleType),
              (acc, ii) => acc + ii.getField("cost") * lit(100000)
            )
          ).otherwise(lit(0))
        ).otherwise(lit(0)).cast(DoubleType)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(LongType)
  }

  def total_cost_microcents(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_impressions_pricing")),
      when(
        coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                   .cast(ByteType),
                 lit(0)
        ).cast(IntegerType) === lit(1),
        coalesce(col(
                   "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
                 ).cast(LongType),
                 lit(0)
        ).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                col(
                  "log_impbus_impressions_pricing.seller_charges.pricing_terms"
                )
              )
            ),
          decimal_round_even(
            f_get_seller_fees(
              col("log_impbus_impressions_pricing.seller_charges.pricing_terms")
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing")),
          when(
            (coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                        .cast(ByteType),
                      lit(0)
            ).cast(IntegerType) === lit(1))
              .and(
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                ) === lit(1)
              )
              .and(is_not_null(col("log_dw_bid_deal.data_costs"))),
            aggregate(
              f_update_data_costs_deal(
                col("log_dw_bid_deal.data_costs"),
                coalesce(
                  lookup(
                    "sup_bidder_member_sales_tax_rate",
                    coalesce(
                      lookup("sup_common_deal",
                             col("log_impbus_preempt.deal_id").cast(IntegerType)
                      ).getField("member_id"),
                      lit(0)
                    )
                  ).getField("sales_tax_rate_pct"),
                  lit(0)
                )
              ),
              lit(0).cast(DoubleType),
              (acc, ii) => acc + ii.getField("cost") * lit(100000)
            )
          ).otherwise(lit(0))
        ).otherwise(lit(0)).cast(DoubleType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                when(
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  ) === lit(1),
                  col(
                    "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                  ).getField("pricing_terms")
                ).otherwise(
                  col("log_impbus_impressions_pricing.buyer_charges")
                    .getField("pricing_terms")
                )
              )
            ),
          decimal_round_even(
            f_get_total_tech_fees(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(LongType)
  }

  def gross_revenue_microcents(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    when(
      is_not_null(col("log_impbus_impressions_pricing")),
      when(
        coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                   .cast(ByteType),
                 lit(0)
        ).cast(IntegerType) === lit(1),
        coalesce(col(
                   "log_impbus_impressions_pricing.impression_event_pricing.seller_revenue_microcents"
                 ).cast(LongType),
                 lit(0)
        ).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                col(
                  "log_impbus_impressions_pricing.seller_charges.pricing_terms"
                )
              )
            ),
          decimal_round_even(
            f_get_seller_fees(
              col("log_impbus_impressions_pricing.seller_charges.pricing_terms")
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing")),
          when(
            (coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                        .cast(ByteType),
                      lit(0)
            ).cast(IntegerType) === lit(1))
              .and(
                f_transaction_event(
                  col("log_impbus_impressions.buyer_transaction_def"),
                  col("log_impbus_preempt.buyer_transaction_def")
                ) === lit(1)
              )
              .and(is_not_null(col("log_dw_bid_deal.data_costs"))),
            aggregate(
              f_update_data_costs_deal(
                col("log_dw_bid_deal.data_costs"),
                coalesce(
                  lookup(
                    "sup_bidder_member_sales_tax_rate",
                    coalesce(
                      lookup("sup_common_deal",
                             col("log_impbus_preempt.deal_id").cast(IntegerType)
                      ).getField("member_id"),
                      lit(0)
                    )
                  ).getField("sales_tax_rate_pct"),
                  lit(0)
                )
              ),
              lit(0).cast(DoubleType),
              (acc, ii) => acc + ii.getField("cost") * lit(100000)
            )
          ).otherwise(lit(0))
        ).otherwise(lit(0)).cast(DoubleType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                when(
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  ) === lit(1),
                  col(
                    "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                  ).getField("pricing_terms")
                ).otherwise(
                  col("log_impbus_impressions_pricing.buyer_charges")
                    .getField("pricing_terms")
                )
              )
            ),
          decimal_round_even(
            f_get_total_tech_fees(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                when(
                  f_should_process_views(
                    col("log_dw_view"),
                    f_transaction_event(
                      col("log_impbus_impressions.seller_transaction_def"),
                      col("log_impbus_preempt.seller_transaction_def")
                    ),
                    f_transaction_event(
                      col("log_impbus_impressions.buyer_transaction_def"),
                      col("log_impbus_preempt.buyer_transaction_def")
                    )
                  ) === lit(1),
                  col(
                    "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                  ).getField("pricing_terms")
                ).otherwise(
                  col("log_impbus_impressions_pricing.buyer_charges")
                    .getField("pricing_terms")
                )
              )
            ),
          decimal_round_even(
            f_get_curator_margin(
              when(
                f_should_process_views(
                  col("log_dw_view"),
                  f_transaction_event(
                    col("log_impbus_impressions.seller_transaction_def"),
                    col("log_impbus_preempt.seller_transaction_def")
                  ),
                  f_transaction_event(
                    col("log_impbus_impressions.buyer_transaction_def"),
                    col("log_impbus_preempt.buyer_transaction_def")
                  )
                ) === lit(1),
                col(
                  "log_impbus_auction_event.auction_event_pricing.buyer_charges"
                ).getField("pricing_terms")
              ).otherwise(
                col("log_impbus_impressions_pricing.buyer_charges")
                  .getField("pricing_terms")
              )
            ) * lit(100000.0d),
            0
          )
        ).otherwise(lit(0)).cast(LongType) + when(
          is_not_null(col("log_impbus_impressions_pricing"))
            .and(
              coalesce(col(
                         "log_impbus_impressions_pricing.seller_charges.is_dw"
                       ).cast(ByteType),
                       lit(0)
              ).cast(IntegerType) === lit(1)
            )
            .and(
              is_not_null(
                when(
                  is_not_null(col("log_dw_bid_curator.data_costs")),
                  f_update_data_costs_deal(
                    col("log_dw_bid_curator.data_costs"),
                    coalesce(
                      lookup(
                        "sup_bidder_member_sales_tax_rate",
                        when(
                          coalesce(col("log_impbus_preempt.curated_deal_id")
                                     .cast(IntegerType),
                                   lit(0)
                          ) =!= lit(0),
                          coalesce(lookup("sup_common_deal",
                                          col(
                                            "log_impbus_preempt.curated_deal_id"
                                          ).cast(IntegerType)
                                   ).getField("member_id"),
                                   lit(0)
                          )
                        ).otherwise(lit(0)).cast(IntegerType)
                      ).getField("sales_tax_rate_pct"),
                      lit(0)
                    )
                  )
                ).otherwise(
                  lit(null).cast(
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
                    )
                  )
                )
              )
            ),
          aggregate(
            when(
              is_not_null(col("log_dw_bid_curator.data_costs")),
              f_update_data_costs_deal(
                col("log_dw_bid_curator.data_costs"),
                coalesce(
                  lookup(
                    "sup_bidder_member_sales_tax_rate",
                    when(
                      coalesce(col("log_impbus_preempt.curated_deal_id").cast(
                                 IntegerType
                               ),
                               lit(0)
                      ) =!= lit(0),
                      coalesce(lookup("sup_common_deal",
                                      col("log_impbus_preempt.curated_deal_id")
                                        .cast(IntegerType)
                               ).getField("member_id"),
                               lit(0)
                      )
                    ).otherwise(lit(0)).cast(IntegerType)
                  ).getField("sales_tax_rate_pct"),
                  lit(0)
                )
              )
            ).otherwise(
              lit(null).cast(
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
                )
              )
            ),
            lit(0.0d).cast(DoubleType),
            (acc, ii) => acc + ii.getField("cost") * lit(100000)
          )
        ).otherwise(lit(0.0d)).cast(DoubleType) + col(
          "log_dw_bid_curator.crossdevice_graph_cost.cost_cpm_usd"
        ) * lit(100000.0d)
      ).otherwise(lit(0))
    ).otherwise(lit(0)).cast(LongType)
  }

  def data_costs(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    coalesce(
      when(
        when(
          is_not_null(col("log_impbus_impressions_pricing")),
          when(
            coalesce(col("log_impbus_impressions_pricing.seller_charges.is_dw")
                       .cast(ByteType),
                     lit(0)
            ).cast(IntegerType) === lit(1),
            f_transaction_event(
              col("log_impbus_impressions.buyer_transaction_def"),
              col("log_impbus_preempt.buyer_transaction_def")
            )
          ).otherwise(lit(0))
        ).otherwise(lit(0)).cast(IntegerType) =!= lit(1),
        lit(null)
      ),
      when(
        is_not_null(col("log_dw_bid_curator.data_costs")),
        f_update_data_costs_deal(
          col("log_dw_bid_curator.data_costs"),
          coalesce(
            lookup(
              "sup_bidder_member_sales_tax_rate",
              when(
                coalesce(
                  col("log_impbus_preempt.curated_deal_id").cast(IntegerType),
                  lit(0)
                ) =!= lit(0),
                coalesce(lookup("sup_common_deal",
                                col("log_impbus_preempt.curated_deal_id").cast(
                                  IntegerType
                                )
                         ).getField("member_id"),
                         lit(0)
                )
              ).otherwise(lit(0)).cast(IntegerType)
            ).getField("sales_tax_rate_pct"),
            lit(0)
          )
        )
      ).otherwise(
        lit(null).cast(
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
          )
        )
      )
    )
  }

}
