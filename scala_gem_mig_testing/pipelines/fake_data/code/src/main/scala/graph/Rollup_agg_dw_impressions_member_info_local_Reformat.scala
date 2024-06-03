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

object Rollup_agg_dw_impressions_member_info_local_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("member_id").cast(IntegerType).as("member_id"),
              col("member_count").cast(LongType).as("member_count")
    )

}
