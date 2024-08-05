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

object optional_filter_and_audit_fields {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#####################################################")
    println("#####Step name: optional_filter_and_audit_fields#####")
    println("#####################################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val out0 = if (spark.conf.get("new_data_flag") == "true") {
    
      val audit_values =
        if (Config.incremental_watermark_max_ts_expr != "None") {
          in0
            .selectExpr(
              "coalesce(min(file_name_timestamp), '-') as min_ts",
              s"coalesce((${Config.incremental_watermark_max_ts_expr}), '-') as max_ts",
              "count(*) as row_count",
              "count(distinct file_name_timestamp) as processed_file_count"
            )
            .collect()(0)
        } else {
          in0
            .selectExpr(
              "coalesce(min(file_name_timestamp), '-') as min_ts",
              "coalesce(max(file_name_timestamp), '-') as max_ts",
              "count(*) as row_count",
              "count(distinct file_name_timestamp) as processed_file_count"
            )
            .collect()(0)
        }
    
      println("source record count: " + audit_values(2).toString)
      println(s"Next Watermark Value will be : ${audit_values(1).toString} ")
    
      spark.conf.set("source_count", audit_values(2).toString)
      spark.conf.set("min_processed_file_timestamp", "-")
      spark.conf.set("max_processed_file_timestamp", "-")
      spark.conf.set("processed_file_count", "0")
    
      if (Config.read_incremental_files_flag == "true") {
    
        spark.conf.set("min_processed_file_timestamp", audit_values(0).toString)
        spark.conf.set("max_processed_file_timestamp", audit_values(1).toString)
        spark.conf.set("processed_file_count", audit_values(3).toString)
      }
      val res = if (Config.source_filter.toLowerCase().contains("partition by")) {
        val where_cond = Config.source_filter
        val window_arr = where_cond.split("==")
        in0
          .withColumn("window_temp_col", expr(window_arr(0).trim()))
          .where("window_temp_col == " + window_arr(1).trim())
          .drop("window_temp_col")
      } else if (Config.source_filter != "None") {
        in0.where(Config.source_filter)
      } else {
        in0.drop("ids_create_run_id")
      }
    
      if (Config.source_filter != "None") {
        spark.conf.set(
          "source_filter_count",
          (res.count()).toString
        )
      } else {
        spark.conf.set("source_filter_count", audit_values(2).toString)
      }
      res
    } else {
      spark.conf.set("min_processed_file_timestamp", "-")
      spark.conf.set("max_processed_file_timestamp", "-")
      spark.conf.set("source_count", "0")
      spark.conf.set("processed_file_count", "0")
      spark.conf.set("source_filter_count", "0")
    
      in0
    
    }
    out0
  }

}
