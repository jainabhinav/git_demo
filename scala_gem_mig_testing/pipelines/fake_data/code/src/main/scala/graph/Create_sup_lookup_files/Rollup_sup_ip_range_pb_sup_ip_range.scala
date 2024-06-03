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

object Rollup_sup_ip_range_pb_sup_ip_range {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("start_ip_number").cast(IntegerType).as("start_ip_number"),
               col("end_ip_number").cast(IntegerType).as("end_ip_number")
      )
      .agg(
        max(col("index")).cast(IntegerType).as("index"),
        max(col("start_ip")).as("start_ip"),
        max(col("end_ip")).as("end_ip"),
        max(col("name")).as("name"),
        max(col("ip_feature")).cast(IntegerType).as("ip_feature")
      )

}
