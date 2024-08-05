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

object update_inc_metadata_table {
  def apply(context: Context, in0: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    println("##############################################")
    println("#####Step name: update_inc_metadata_table#####")
    println("##############################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val accumulated_process_flag = if (Config.accumulated_process_flag == "true") true else false
    
    if (
      Config.read_incremental_files_flag == "true" && spark.conf.get(
        "new_data_flag"
      ) == "true"
    ) {
      if (!accumulated_process_flag) {
        println("Updating incremental metadata table")
        import _root_.io.delta.tables._
        import spark.implicits._
        val metaColumns = Seq(
          "data_mart",
          "pipeline_name",
          "source",
          "target",
          "last_process_timestamp"
        )
    
        val metaDataDf = spark
          .createDataFrame(
            Seq(
              (
                Config.data_mart,
                Config.pipeline_name,
                Config.source_table,
                Config.target_table,
                spark.conf.get("max_processed_file_timestamp")
              )
            )
          )
          .toDF(metaColumns: _*)
        // val metaDataDf = in0
        //   .groupBy(lit(Config.pipeline_name), col("source_file_base_path"))
        //   .agg(
        //     max("file_name_timestamp")
        //       .alias("last_process_timestamp")
        //   )
        //   .toDF(metaColumns: _*)
    
        if (!spark.catalog.tableExists(Config.incremental_load_metadata_table)) {
          metaDataDf.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("pipeline_name")
            .saveAsTable(Config.incremental_load_metadata_table)
        } else {
          DeltaTable
            .forName(Config.incremental_load_metadata_table)
            .as("target")
            .merge(
              metaDataDf.as("source"),
              (col("target.`pipeline_name`") === lit(Config.pipeline_name)) &&
                (col("source.`target`") === col("target.`target`"))
            )
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()
        }
      } else {
        if (accumulated_process_flag) {
          val run_id = spark.conf.get("run_id")
          val current_ts_val = to_timestamp(
            from_utc_timestamp(current_timestamp(), "America/Chicago")
              .cast("string"),
            "yyyy-MM-dd HH:mm:ss"
          )
          val new_files =
            spark.conf.get("accoumulated_process_files").split(",").toList
          import spark.implicits._
          val write_df = new_files
            .toDF("processed_file")
            .withColumn("data_mart", lit(Config.data_mart))
            .withColumn("pipeline_name", lit(Config.pipeline_name))
            .withColumn("target", lit(Config.target_table))
            .withColumn("run_id", lit(run_id).cast("long"))
            .withColumn("insert_ts", current_ts_val)
            .select("data_mart","pipeline_name", "target", "run_id", "processed_file", "insert_ts")
    
          write_df.write
            .format("delta")
            .mode("append")
            .partitionBy("pipeline_name")
            .option(
              "optimizeWrite",
              true
            )
            .option(
              "delta.enableChangeDataFeed",
              true
            )
            .saveAsTable(Config.accumulated_processed_files_table)
        }
    
      }
    }
  }

}
