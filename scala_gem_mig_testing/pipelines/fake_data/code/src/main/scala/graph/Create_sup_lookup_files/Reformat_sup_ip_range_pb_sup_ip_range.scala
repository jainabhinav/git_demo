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

object Reformat_sup_ip_range_pb_sup_ip_range {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("index").cast(IntegerType).as("index"),
      col("start_ip"),
      col("end_ip"),
      col("name"),
      col("ip_feature").cast(IntegerType).as("ip_feature"),
      concat(
        element_at(split(col("start_ip"), "\\."), 1),
        string_lpad(element_at(split(col("start_ip"), "\\."), 2), 3, "0")
          .cast(StringType),
        string_lpad(element_at(split(col("start_ip"), "\\."), 3), 3, "0")
          .cast(StringType)
      ).cast(IntegerType).as("start_ip_number"),
      concat(
        element_at(split(col("end_ip"), "\\."), 1),
        string_lpad(element_at(split(col("end_ip"), "\\."), 2), 3, "0")
          .cast(StringType),
        string_lpad(element_at(split(col("end_ip"), "\\."), 3), 3, "0")
          .cast(StringType)
      ).cast(IntegerType).as("end_ip_number")
    )

}
