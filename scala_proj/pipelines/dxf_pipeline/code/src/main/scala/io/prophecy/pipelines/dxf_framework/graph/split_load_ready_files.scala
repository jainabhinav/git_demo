package io.prophecy.pipelines.dxf_framework.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config.Context
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object split_load_ready_files {
  def apply(context: Context, in0: DataFrame): (DataFrame, DataFrame) = {
    val spark = context.spark
    val Config = context.config
    println("###########################################")
    println("#####Step name: split_load_ready_files#####")
    println("###########################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    var out0 = in0
    var out1 = in0
    
    spark.conf.set("insert_load_ready_count", "0")
    spark.conf.set("update_load_ready_count", "0")
    if (
      (Config.skip_delta_synapse_write_common_dimension == "true" && spark.conf
        .get(
          "new_data_flag"
        ) == "true" && Config.generate_load_ready_files == "true" && Config.skip_main_table_load_ready_files == "false")
    ) {
      out0 = in0
      spark.conf.set("insert_load_ready_count", out0.count().toString)
      out1 = in0.where("1==2")
    } else if (
      (Config.generate_load_ready_files == "true" && spark.conf.get(
        "new_data_flag"
      ) == "true" && Config.skip_main_table_load_ready_files == "false") || (Config.generate_only_load_ready_files == "true" && Config.skip_main_table_load_ready_files == "false")
    ) {
      out0 = in0.filter(col("_change_type") === lit("insert"))
      spark.conf.set("insert_load_ready_count", out0.count().toString)
      out1 = in0.filter(col("_change_type") === lit("update_postimage"))
      spark.conf.set("update_load_ready_count", out1.count().toString)
    }
    (out0, out1)
  }

}
