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

object Sort_sup_buy_side_object_hierarchy_pb_advertiser_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("advertiser_id").asc)

}
