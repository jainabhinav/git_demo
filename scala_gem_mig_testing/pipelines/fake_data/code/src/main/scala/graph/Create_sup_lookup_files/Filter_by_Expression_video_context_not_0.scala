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

object Filter_by_Expression_video_context_not_0 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      is_not_null(col("video_context")).and(col("video_context") =!= lit(0))
    )

}
