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

object Rollup_video_slot {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(max(col("date_time")).cast(LongType).as("date_time"),
           max(col("video_context")).cast(IntegerType).as("video_context")
      )

}
