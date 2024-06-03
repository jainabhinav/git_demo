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

object Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("id").cast(IntegerType).as("id"))
      .agg(
        max(col("supports_skippable"))
          .cast(IntegerType)
          .as("supports_skippable"),
        max(col("max_duration_secs")).cast(IntegerType).as("max_duration_secs"),
        max(col("max_ad_duration_secs"))
          .cast(IntegerType)
          .as("max_ad_duration_secs"),
        max(col("maximum_number_ads"))
          .cast(IntegerType)
          .as("maximum_number_ads"),
        max(col("start_delay_secs")).cast(IntegerType).as("start_delay_secs"),
        max(col("playback_method")).cast(IntegerType).as("playback_method"),
        max(col("video_context")).cast(IntegerType).as("video_context"),
        max(col("is_mediated")).cast(IntegerType).as("is_mediated"),
        max(col("skip_offset")).cast(IntegerType).as("skip_offset")
      )

}
