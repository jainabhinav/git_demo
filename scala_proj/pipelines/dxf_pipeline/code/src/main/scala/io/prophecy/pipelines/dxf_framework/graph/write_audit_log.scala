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

object write_audit_log {
  def apply(context: Context, in0: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    // this writes the audit summary for the run
    println("####################################")
    println("#####Step name: write_audit_log#####")
    println("####################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    if (Config.disable_audit_summary == "false") {
      println("Writing audit summary for pipeline run")
      val target_tbl = Config.target_table_db + "." + Config.target_table
    
      val run_id = spark.conf.get("run_id").toLong
      val source_count = spark.conf.get("source_count").toLong
      val source_filter_count =
        source_count - spark.conf.get("source_filter_count").toLong
      val reject_record_count = spark.conf.get("reject_record_count").toLong
      val duplicate_filtered_record_count =
        spark.conf.get("source_filter_count").toLong - reject_record_count - spark.conf
          .get("duplicate_filtered_record_count")
          .toLong
      val post_filtered_record_count = spark.conf.get("duplicate_filtered_record_count").toLong - spark.conf
        .get("post_filtered_record_count")
        .toLong
    
      val insert_load_ready_count = spark.conf.get("insert_load_ready_count").toLong
      val update_load_ready_count = spark.conf.get("update_load_ready_count").toLong
    
      val min_processed_file_timestamp =
        spark.conf.get("min_processed_file_timestamp")
      val max_processed_file_timestamp =
        spark.conf.get("max_processed_file_timestamp")
      val processed_file_count = spark.conf.get("processed_file_count").toInt
    
      val auditColumns = Seq(
        "data_mart",
        "pipeline_name",
        "run_id",
        "source_count",
        "pre_filter_record_count",
        "reject_record_count",
        "deduplicate_record_count",
        "post_filter_record_count",
        "insert_load_ready_count",
        "update_load_ready_count",
        "min_processed_file_timestamp",
        "max_processed_file_timestamp",
        "processed_file_count"
      )
    
      val auditDf = spark
        .createDataFrame(
          Seq(
            (
              Config.data_mart,
              Config.pipeline_name,
              run_id,
              source_count,
              source_filter_count,
              reject_record_count,
              duplicate_filtered_record_count,
              post_filtered_record_count,
              insert_load_ready_count,
              update_load_ready_count,
              min_processed_file_timestamp,
              max_processed_file_timestamp,
              processed_file_count
            )
          )
        )
        .toDF(auditColumns: _*)
    
      // write audit summary output to delta table
      auditDf.write
        .format("delta")
        .option("delta.enableChangeDataFeed", true)
        .partitionBy("pipeline_name")
        .mode("append")
        .saveAsTable(Config.audit_summary)
    
      println("Process completed for table:" + Config.target_table)
    }
    spark.sqlContext.clearCache()
  }

}
