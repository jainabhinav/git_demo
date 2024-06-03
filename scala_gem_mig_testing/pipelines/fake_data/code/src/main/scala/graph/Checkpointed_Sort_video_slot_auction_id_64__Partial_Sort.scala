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

object Checkpointed_Sort_video_slot_auction_id_64__Partial_Sort {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("auction_id_64").asc)

}
