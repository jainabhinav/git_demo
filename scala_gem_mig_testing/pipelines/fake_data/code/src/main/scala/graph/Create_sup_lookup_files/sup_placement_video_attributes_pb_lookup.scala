package graph.Create_sup_lookup_files

import io.prophecy.libs._
import graph.Create_sup_lookup_files.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object sup_placement_video_attributes_pb_lookup {

  def apply(context: Context, in: DataFrame): Unit =
    createLookup(
      "sup_placement_video_attributes_pb",
      in,
      context.spark,
      List("id"),
      "id",
      "supports_skippable",
      "max_duration_secs",
      "max_ad_duration_secs",
      "maximum_number_ads",
      "start_delay_secs",
      "playback_method",
      "video_context",
      "is_mediated",
      "skip_offset"
    )

}
