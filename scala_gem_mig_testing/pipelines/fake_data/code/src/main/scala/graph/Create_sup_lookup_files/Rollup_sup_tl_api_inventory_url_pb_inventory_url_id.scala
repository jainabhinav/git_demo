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

object Rollup_sup_tl_api_inventory_url_pb_inventory_url_id {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("inventory_url_id").cast(IntegerType).as("inventory_url_id"))
      .agg(max(col("inventory_url")).as("inventory_url"))

}
