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

object optional_filter_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("##########################################")
    println("#####Step name: optional_filter_rules#####")
    println("##########################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    val out0 =
      if (
        Config.optional_filter_rules != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        in0.where(Config.optional_filter_rules)
      } else {
        in0
      }
    out0
  }

}
