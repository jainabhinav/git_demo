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

object Reformat_stage_impbus_impression_sample {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      coalesce(col("user_id_64").cast(LongType),          lit(0)).as("user_id_64"),
      coalesce(col("seller_member_id").cast(IntegerType), lit(0))
        .as("seller_member_id"),
      lit(0).as("inventory_source_id"),
      coalesce(col("tag_id").cast(IntegerType),       lit(0)).as("tag_id"),
      coalesce(col("venue_id").cast(IntegerType),     lit(0)).as("venue_id"),
      coalesce(col("publisher_id").cast(IntegerType), lit(0))
        .as("publisher_id"),
      coalesce(col("site_domain"),                           lit("---")).as("site_domain"),
      coalesce(col("content_category_id").cast(IntegerType), lit(0))
        .as("content_category_id"),
      coalesce(col("geo_country"),                     lit("--")).as("geo_country"),
      coalesce(col("user_group_id").cast(IntegerType), lit(0))
        .as("user_group_id"),
      coalesce(col("operating_system").cast(IntegerType), lit(0))
        .as("operating_system"),
      coalesce(col("geo_region"),                         lit("--")).as("geo_region"),
      coalesce(col("browser").cast(IntegerType),          lit(0)).as("browser"),
      coalesce(col("language").cast(IntegerType),         lit(0)).as("language"),
      coalesce(col("inventory_url_id").cast(IntegerType), lit(0))
        .as("inventory_url_id"),
      coalesce(col("audit_type").cast(IntegerType), lit(0)).as("audit_type"),
      coalesce(col("width").cast(IntegerType),      lit(0)).as("width"),
      coalesce(col("height").cast(IntegerType),     lit(0)).as("height"),
      col("imp_type").cast(IntegerType).as("imp_type"),
      coalesce(col("is_delivered").cast(IntegerType), lit(0))
        .cast(IntegerType)
        .as("is_transacted"),
      coalesce(col("buyer_member_id").cast(IntegerType), lit(0))
        .as("buyer_member_id"),
      coalesce(col("buyer_spend"),                         lit(0.0d)).as("buyer_spend"),
      coalesce(col("ip_address"),                          lit("")).as("ip_address"),
      coalesce(col("vp_expose_domains").cast(IntegerType), lit(0))
        .as("vp_expose_domains"),
      coalesce(col("vp_expose_pubs").cast(IntegerType), lit(1))
        .as("vp_expose_pubs"),
      coalesce(col("visibility_profile_id").cast(IntegerType), lit(0))
        .as("visibility_profile_id"),
      coalesce(col("is_exclusive").cast(IntegerType), lit(0))
        .as("is_exclusive"),
      coalesce(col("device_id").cast(IntegerType),     lit(0)).as("device_id"),
      coalesce(col("supply_type").cast(IntegerType),   lit(0)).as("supply_type"),
      coalesce(col("truncate_ip").cast(IntegerType),   lit(0)).as("truncate_ip"),
      coalesce(col("application_id"),                  lit("---")).as("application_id"),
      coalesce(col("device_type").cast(IntegerType),   lit(0)).as("device_type"),
      coalesce(col("datacenter_id").cast(IntegerType), lit(0))
        .as("datacenter_id"),
      coalesce(col("media_type").cast(IntegerType), lit(0)).as("media_type"),
      coalesce(col("site_id").cast(IntegerType),    lit(0)).as("site_id"),
      coalesce(col("allowed_media_types"),          array()).as("allowed_media_types"),
      col("imp_ignored").cast(ByteType).as("imp_ignored"),
      col("imp_blacklist_or_fraud")
        .cast(IntegerType)
        .as("imp_blacklist_or_fraud"),
      col("anonymized_user_info"),
      col("cookie_age").cast(IntegerType).as("cookie_age")
    )

}
