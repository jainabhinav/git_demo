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

object Rollup_sup_common_deal_pb_sup_common_deal_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("id").cast(IntegerType).as("id"),
              col("member_id").cast(IntegerType).as("member_id"),
              col("deal_type_id").cast(IntegerType).as("deal_type_id")
    )

}
