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

object Rollup_sup_sell_side_object_hierarchy_pb_tag_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("tag_id").cast(IntegerType).as("tag_id"))
      .agg(
        max(col("seller_member_id")).cast(IntegerType).as("seller_member_id"),
        max(col("publisher_id")).cast(IntegerType).as("publisher_id"),
        max(col("site_id")).cast(IntegerType).as("site_id")
      )

}
