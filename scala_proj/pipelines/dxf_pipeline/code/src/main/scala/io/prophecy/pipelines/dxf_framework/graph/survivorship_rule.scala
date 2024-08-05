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

object survivorship_rule {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("######################################")
    println("#####Step name: survivorship_rule#####")
    println("######################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val isPKSet = spark.conf.getAll.contains("primary_key")
    val prim_key_columns = if (isPKSet) {
      spark.conf
        .get("primary_key")
        .split(",")
        .map(_.trim.toLowerCase)
    } else {
      Config.primary_key
        .split(",")
        .map(_.trim.toLowerCase)
    }
    
    var out0 =
      if (
        Config.survivorship_flag && !prim_key_columns
          .contains("src_env_sk") &&
        spark.catalog.tableExists(
          s"${Config.target_table_db}.${Config.target_table}"
        ) &&
        spark.conf.get("new_data_flag") == "true"
      ) {
    
        val tgt_tbl_nm =
          if (
            Config.survivorship_override_tables
              .split(",")
              .map(_.trim)
              .contains(Config.target_table)
          ) Config.target_table
          else "-"
        val srcEnvRnk = lookup(
          "lookup_src_envrt_id",
          col(Config.survivorship_lookup_column).cast(StringType),
          lit(tgt_tbl_nm)
        ).getField("src_env_rnk")
        val in1SrcEnvRnk = lookup(
          "lookup_src_envrt_id",
          col("in1_src_env_sk").cast(StringType),
          lit(tgt_tbl_nm)
        ).getField("src_env_rnk")
    
        in0
          .withColumn("src_env_rnk", coalesce(srcEnvRnk, lit(9999)))
          .withColumn("in1_src_env_rnk", coalesce(in1SrcEnvRnk, lit(9999)))
          .where("cast (src_env_rnk as long)  <= cast(in1_src_env_rnk as long)")
     
      } else {
        in0
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
