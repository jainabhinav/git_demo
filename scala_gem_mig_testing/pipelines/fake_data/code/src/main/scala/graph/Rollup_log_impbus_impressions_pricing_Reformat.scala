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

object Rollup_log_impbus_impressions_pricing_Reformat {

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
      col("log_impbus_impressions_pricing_dup")
    )

}
