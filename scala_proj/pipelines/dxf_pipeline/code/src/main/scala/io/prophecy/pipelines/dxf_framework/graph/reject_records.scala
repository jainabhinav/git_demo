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

object reject_records {
  def apply(context: Context, in0: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    // this method write rejected records to the path provided by config as well as write the reject record summary
    
    println("###################################")
    println("#####Step name: reject_records#####")
    println("###################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    if (
      Config.disable_reject_record_summary == "false" && spark.conf.get(
        "new_data_flag"
      ) == "true"
      && spark.conf.get("reject_record_count") != "0"
    ) {
    
      val run_id = spark.conf.get("run_id")
      // write reject records in delta table
      in0
        .withColumn(
          "run_id",
          lit(run_id)
        )
        .write
        .format("delta")
        //.option("optimizeWrite", true)
        .option("delta.enableChangeDataFeed", true)
        .partitionBy("run_id")
        .mode("append")
        .saveAsTable(Config.error_table_prefix + Config.target_table)
    
      // write reject record summary in delta table
      in0
        .withColumn(
          "run_id",
          lit(run_id)
        )
        .groupBy(
          lit(Config.pipeline_name).as("pipeline_name"),
          col("run_id"),
          col("reject_record")
        ) // group by reject record type
        .agg(count(lit(1)).as("reject_count"))
        .withColumn(
          "run_id",
          lit(run_id)
        )
        .write
        .format("delta")
        //.option("optimizeWrite", true)
        .option("delta.enableChangeDataFeed", true)
        .partitionBy("pipeline_name", "run_id")
        .mode("append")
        .saveAsTable(Config.error_summary_table)
    
        val reject_record_count = spark.conf.get("reject_record_count").toInt
    
        if (reject_record_count > Config.reject_record_threshold && Config.length_rules_from_metadata_table != "true"){
        throw new Exception(
            "Reject record count (" + reject_record_count.toString + ") is greater than the defined reject record threshold ("
              + Config.reject_record_threshold.toString + ") for the pipeline"
          )
      }
    
    } else{
      val reject_record_count = spark.conf.get("reject_record_count").toInt
    
        if (reject_record_count > Config.reject_record_threshold && Config.length_rules_from_metadata_table != "true"){
        throw new Exception(
            "Reject record count (" + reject_record_count.toString + ") is greater than the defined reject record threshold ("
              + Config.reject_record_threshold.toString + ") for the pipeline"
          )
      }
    }
  }

}
