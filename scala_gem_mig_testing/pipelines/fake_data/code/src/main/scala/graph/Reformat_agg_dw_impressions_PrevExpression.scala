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

object Reformat_agg_dw_impressions_PrevExpression {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.withColumn(
        "_f_view_detection_enabled",
        f_view_detection_enabled(
          col("log_impbus_impressions.view_detection_enabled").cast(
            IntegerType
          ),
          col("log_impbus_preempt.view_detection_enabled").cast(IntegerType)
        )
      )
      .withColumn(
        "f_transaction_event_type_id_87067",
        f_transaction_event_type_id(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        )
      )
      .withColumn(
        "_f_viewdef_definition_id",
        f_viewdef_definition_id(
          col("log_impbus_impressions.viewdef_definition_id_buyer_member").cast(
            IntegerType
          ),
          col("log_impbus_preempt.viewdef_definition_id_buyer_member").cast(
            IntegerType
          ),
          col("log_impbus_view.viewdef_definition_id").cast(IntegerType)
        )
      )
      .withColumn("_f_is_buy_side",
                  f_is_buy_side(col("log_dw_bid"),
                                col("buyer_member_id").cast(IntegerType),
                                col("member_id").cast(IntegerType)
                  )
      )
      .withColumn(
        "_f_has_transacted",
        f_has_transacted(col("log_impbus_impressions.buyer_transaction_def"),
                         col("log_impbus_preempt.buyer_transaction_def")
        )
      )
      .withColumn(
        "f_transaction_event_87047",
        f_transaction_event(
          col("log_impbus_impressions.seller_transaction_def"),
          col("log_impbus_preempt.seller_transaction_def")
        )
      )
      .withColumn(
        "f_transaction_event_87057",
        f_transaction_event(col("log_impbus_impressions.buyer_transaction_def"),
                            col("log_impbus_preempt.buyer_transaction_def")
        )
      )
      .withColumn(
        "f_preempt_over_impression_95337",
        f_preempt_over_impression(
          col("log_impbus_impressions.creative_media_subtype_id").cast(
            IntegerType
          ),
          col("log_impbus_preempt.creative_media_subtype_id").cast(IntegerType)
        )
      )
      .withColumn(
        "f_transaction_event_type_id_87077",
        f_transaction_event_type_id(
          col("log_impbus_impressions.buyer_transaction_def"),
          col("log_impbus_preempt.buyer_transaction_def")
        )
      )
      .withColumn(
        "f_preempt_over_impression_94298",
        f_preempt_over_impression(
          col("log_impbus_impressions.external_campaign_id"),
          col("log_impbus_preempt.external_campaign_id")
        )
      )
      .withColumn(
        "f_preempt_over_impression_88439",
        f_preempt_over_impression(
          col("log_impbus_impressions.deal_type").cast(IntegerType),
          col("log_impbus_preempt.deal_type").cast(IntegerType)
        )
      )
      .withColumn(
        "f_preempt_over_impression_88639",
        f_preempt_over_impression(
          col("log_impbus_impressions.creative_id").cast(IntegerType),
          col("log_impbus_preempt.creative_id").cast(IntegerType)
        )
      )
      .withColumn(
        "in_f_create_agg_dw_impressions",
        f_create_agg_dw_impressions(
          col("_f_view_detection_enabled"),
          col("f_transaction_event_type_id_87067"),
          col("_f_viewdef_definition_id"),
          col("_f_is_buy_side"),
          col("_f_has_transacted"),
          col("f_transaction_event_87047"),
          col("f_transaction_event_87057"),
          col("f_preempt_over_impression_95337"),
          col("f_transaction_event_type_id_87077"),
          col("f_preempt_over_impression_94298"),
          col("f_preempt_over_impression_88439"),
          col("f_preempt_over_impression_88639")
        )
      )

}
