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

object Inner_Join_log_impbus_impressions_pricing {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            col("left.auction_id_64") === col("right.auction_id_64"),
            "inner"
      )
      .select(
        coalesce(col("left.auction_id_64"),
                 col("right.log_impbus_impressions_pricing.auction_id_64")
        ).as("auction_id_64"),
        coalesce(col("left.date_time"),
                 col("right.log_impbus_impressions_pricing.date_time")
        ).as("date_time"),
        col("left.is_delivered").as("is_delivered"),
        coalesce(
          when(
            is_not_null(
              col("right.log_impbus_impressions_pricing.buyer_charges.is_dw")
            ).and(
              col(
                "right.log_impbus_impressions_pricing.buyer_charges.is_dw"
              ) === lit(1)
            ),
            col("right.log_impbus_impressions_pricing.buyer_charges.is_dw")
          ),
          col("left.is_dw"),
          when(
            is_not_null(
              col("right.log_impbus_impressions_pricing.seller_charges.is_dw")
            ).and(
              col(
                "right.log_impbus_impressions_pricing.seller_charges.is_dw"
              ) === lit(1)
            ),
            col("right.log_impbus_impressions_pricing.seller_charges.is_dw")
          ),
          when(is_not_null(col("left.log_impbus_preempt.is_dw"))
                 .and(col("left.log_impbus_preempt.is_dw") > lit(-1)),
               col("left.log_impbus_preempt.is_dw")
          ),
          when(col("left.log_impbus_impressions.is_dw") > lit(-1),
               col("left.log_impbus_impressions.is_dw")
          ),
          lit(0)
        ).as("is_dw"),
        col("left.seller_member_id").as("seller_member_id"),
        col("left.buyer_member_id").as("buyer_member_id"),
        col("left.member_id").as("member_id"),
        col("left.publisher_id").as("publisher_id"),
        col("left.site_id").as("site_id"),
        col("left.tag_id").as("tag_id"),
        col("left.advertiser_id").as("advertiser_id"),
        col("left.campaign_group_id").as("campaign_group_id"),
        col("left.campaign_id").as("campaign_id"),
        col("left.insertion_order_id").as("insertion_order_id"),
        col("left.imp_type").as("imp_type"),
        col("left.is_transactable").as("is_transactable"),
        col("left.is_transacted_previously").as("is_transacted_previously"),
        col("left.is_deferred_impression").as("is_deferred_impression"),
        col("left.has_null_bid").as("has_null_bid"),
        col("left.log_impbus_impressions").as("log_impbus_impressions"),
        col("left.log_impbus_preempt_count").as("log_impbus_preempt_count"),
        col("left.log_impbus_preempt").as("log_impbus_preempt"),
        col("left.log_impbus_preempt_dup").as("log_impbus_preempt_dup"),
        coalesce(col("left.log_impbus_impressions_pricing_count"),
                 col("right.log_impbus_impressions_pricing_count")
        ).as("log_impbus_impressions_pricing_count"),
        col("right.log_impbus_impressions_pricing")
          .as("log_impbus_impressions_pricing"),
        col("right.log_impbus_impressions_pricing_dup")
          .as("log_impbus_impressions_pricing_dup")
      )

}
