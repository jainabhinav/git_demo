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

object optional_normalize {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#######################################")
    println("#####Step name: optional_normalize#####")
    println("#######################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    val out0 = if (Config.normalize_rules != "None" && spark.conf.get("new_data_flag") == "true") {
      in0.withColumn("normalized_col", explode_outer(expr(Config.normalize_rules)))
    } else {
      in0
    }
    out0
  }

}
