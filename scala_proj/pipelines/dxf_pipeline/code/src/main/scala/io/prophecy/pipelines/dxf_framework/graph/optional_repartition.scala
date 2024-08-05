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

object optional_repartition {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#########################################")
    println("#####Step name: optional_repartition#####")
    println("#########################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val out0 =
      if (
        Config.repartition_flag == "true" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
        in0.repartitionByRange(
          Config.repartition_cols.split(",").map(x => col(x.trim())): _*
        )
      } else if (
        Config.enable_round_robin_partioning && Config.spark_configs != "None" && spark.conf
          .get("new_data_flag") == "true"
      ) {
        val numPartitions = Config.spark_configs
          .split(",")
          .filter(_.contains("shuffle"))
          .array(0)
          .split("=")
          .array(1).toInt
        println(
          "round_robin_partioning is enabled, number of partitions: " + numPartitions
        )
        in0.repartition(numPartitions)
      } else {
        in0
      }
    out0
  }

}
