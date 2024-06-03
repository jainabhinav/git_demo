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

object Filter_by_Expression_agg_dw_curator_impressions {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      xcdf_member_owner_object_no_imp_type_check(
        col("member_id").cast(IntegerType),
        col("seller_member_id").cast(IntegerType),
        col("advertiser_id").cast(IntegerType),
        col("campaign_group_id").cast(IntegerType),
        col("publisher_id").cast(IntegerType),
        lookup_member_id_by_advertiser_id(lit("member_id_by_advertiser_id"),
                                          col("advertiser_id").cast(IntegerType)
        ).getField("buyer_member_id"),
        lookup_advertiser_id_by_campaign_group_id(
          lit("advertiser_id_by_campaign_group_id"),
          col("campaign_group_id").cast(IntegerType)
        ).getField("advertiser_id"),
        lookup_member_id_by_publisher_id(lit("member_id_by_publisher_id"),
                                         col("publisher_id").cast(IntegerType)
        ).getField("seller_member_id")
      ) === lit(1)
    )

}
