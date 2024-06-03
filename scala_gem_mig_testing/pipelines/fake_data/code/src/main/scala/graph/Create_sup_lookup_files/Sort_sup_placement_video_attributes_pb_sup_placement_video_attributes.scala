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

object Sort_sup_placement_video_attributes_pb_sup_placement_video_attributes {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("id").asc)

}
