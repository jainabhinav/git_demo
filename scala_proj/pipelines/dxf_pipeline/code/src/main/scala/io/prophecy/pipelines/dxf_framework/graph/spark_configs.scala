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

object spark_configs {
  def apply(context: Context): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("##################################")
    println("#####Step name: spark_configs#####")
    println("##################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    val sparkConfigs = Config.spark_configs.split(",")
    
    val sparkSession = sparkConfigs
      .foldLeft(SparkSession.builder().appName("Prophecy Pipeline")) {
        case (s, configStr) =>
          val Array(key, value) = configStr.split("=").map(_.trim)
          s.config(key, value)
      }
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    
    val out0 = spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
    out0
  }

}
