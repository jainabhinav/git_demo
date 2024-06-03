package graph.Create_sup_lookup_files

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_sup_buy_side_object_hierarchy_pb_advertiser_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("advertiser_id").cast(IntegerType).as("advertiser_id"))
      .agg(
        max(col("buyer_member_id")).cast(IntegerType).as("buyer_member_id"),
        max(col("campaign_group_id")).cast(IntegerType).as("campaign_group_id"),
        max(col("campaign_id")).cast(IntegerType).as("campaign_id")
      )

}
