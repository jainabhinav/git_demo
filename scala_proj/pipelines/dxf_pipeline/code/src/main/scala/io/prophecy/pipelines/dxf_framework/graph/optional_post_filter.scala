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

object optional_post_filter {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#########################################")
    println("#####Step name: optional_post_filter#####")
    println("#########################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    val out0 =
      if (
        Config.target_filter != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        try{
          in0.where(Config.target_filter)
        }catch {
          case e: Exception => {
            println(
              s"""Process failed while applying post filter"""
            )
    
            if (spark.conf.get("main_table_api_type") == "NON-API"){
              import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
              println(s" Removing lock from: ${Config.sk_table_name_override}")
              dbutils.fs.rm(Config.sk_lock_file_path + Config.sk_table_name_override + ".txt")
            }
            throw e
          }
        }
      } else {
        in0
      }
    
    if (
      Config.target_filter != "None" && spark.conf.get("new_data_flag") == "true"
    ) {
      spark.conf.set("post_filtered_record_count", out0.count().toString)
    } else {
      spark.conf.set("post_filtered_record_count", spark.conf.get("source_count"))
    }
    
    if(Config.debug_flag) {
      var printDf = out0
      if(Config.debug_filter.toLowerCase() != "none"){
        printDf = printDf.where(Config.debug_filter)
      }  
      if(Config.debug_col_list.toLowerCase() != "none"){
        val print_cols = Config.debug_col_list.split(",").map(x => x.trim())
        printDf.selectExpr(print_cols : _*).show(truncate=false)
      } else {
        printDf.show(truncate=true)
      }
    }
    out0
  }

}
