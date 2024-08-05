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

object put_src_env_sk {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("###################################")
    println("#####Step name: put_src_env_sk#####")
    println("###################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    val out0 = if (spark.conf.get("new_data_flag") == "true") {
      if (in0.columns.contains("dxf_src_sys_id")) {
        in0
      } else if (in0.columns.contains("src_env_sk")) {
        in0.withColumn("dxf_src_sys_id", col("src_env_sk"))
      } else {
        in0.withColumn("dxf_src_sys_id", lit(Config.src_env_sk).cast("string"))
      }
    } else {
      in0
    }
    out0
  }

}
