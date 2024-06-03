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

object Rollup_log_impbus_preempt_currentRow_0 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
        "w_1463911",
        min(col("accept_timestamp")).over(
          Window
            .partitionBy(col("auction_id_64"))
            .orderBy(lit(1).asc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .withColumn(
        "w_1463972",
        last(
          when(
            col("accept_timestamp") === col("w_1463911"),
            struct(
              col("date_time"),
              col("auction_id_64"),
              col("imp_transacted"),
              col("buyer_spend"),
              col("seller_revenue"),
              col("bidder_fees"),
              col("instance_id"),
              col("fold_position"),
              col("seller_deduction"),
              col("buyer_member_id"),
              col("creative_id"),
              col("cleared_direct"),
              col("buyer_currency"),
              col("buyer_exchange_rate"),
              col("width"),
              col("height"),
              col("brand_id"),
              col("creative_audit_status"),
              col("is_creative_hosted"),
              col("vp_expose_domains"),
              col("vp_expose_categories"),
              col("vp_expose_pubs"),
              col("vp_expose_tag"),
              col("bidder_id"),
              col("deal_id"),
              col("imp_type"),
              col("is_dw"),
              col("vp_bitmap"),
              col("ttl"),
              col("view_detection_enabled"),
              col("media_type"),
              col("auction_timestamp"),
              col("spend_protection"),
              col("viewdef_definition_id_buyer_member"),
              col("deal_type"),
              col("ym_floor_id"),
              col("ym_bias_id"),
              col("bid_price_type"),
              col("spend_protection_pixel_id"),
              col("ip_address"),
              col("buyer_transaction_def"),
              col("seller_transaction_def"),
              col("buyer_bid"),
              col("expected_events"),
              col("accept_timestamp"),
              col("external_creative_id"),
              col("seat_id"),
              col("is_prebid_server"),
              col("curated_deal_id"),
              col("external_campaign_id"),
              col("trust_id"),
              col("log_product_ads"),
              col("external_bidrequest_id"),
              col("external_bidrequest_imp_id"),
              col("creative_media_subtype_id")
            )
          )
        ).over(
          Window
            .partitionBy(col("auction_id_64"))
            .orderBy(lit(1).asc)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
  }

}
