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

object create_delta_file {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("######################################")
    println("#####Step name: create_delta_file#####")
    println("######################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val target_tbl = Config.target_table_db + "." + Config.target_table
    var maxVersion = -1
    var maxVersionPostSQLUpdate = -1
    
    if (
      (Config.generate_load_ready_files == "true" && spark.conf.get(
        "new_data_flag"
      ) == "true" && Config.skip_delta_synapse_write_common_dimension == "false" && Config.skip_main_table_load_ready_files == "false") || (Config.generate_only_load_ready_files == "true")
    ) {
      maxVersion = spark
        .sql(
          "select max(version) from (DESCRIBE HISTORY " + target_tbl + ") temp where lower(temp.operation) <> 'optimize'"
        )
        .collect()(0)
        .getLong(0)
        .toInt
    }
    maxVersionPostSQLUpdate = maxVersion
    if (Config.post_delta_sql_update != "None") {
      println("Running post SQL Query")
      spark.sql(Config.post_delta_sql_update)
      maxVersionPostSQLUpdate = spark
        .sql(
          "select max(version) from (DESCRIBE HISTORY " + target_tbl + ") temp where lower(temp.operation) <> 'optimize'"
        )
        .collect()(0)
        .getLong(0)
        .toInt
    
    }
    
    val temp_out0 =
      if (
        (Config.generate_load_ready_files == "true" && spark.conf.get(
          "new_data_flag"
        ) == "true" && Config.skip_delta_synapse_write_common_dimension == "false" && Config.skip_main_table_load_ready_files == "false") || (Config.generate_only_load_ready_files == "true")
      ) {
    
    // read cdc data from delta table
        if (Config.post_delta_sql_update != "None") {
          import org.apache.spark.sql.expressions.Window
          println("post sql deduplicate")
    
          val isPKSet = spark.conf.getAll.contains("primary_key")
          val prim_key_columns = if (isPKSet) {
            spark.conf
              .get("primary_key")
              .split(",")
              .map(x => x.trim())
          } else {
            Config.primary_key
              .split(",")
              .map(x => x.trim())
          }
    
          val temp_df = spark.read
            .format("delta")
            .option("readChangeData", true)
            .option("startingVersion", maxVersion)
            .option("endingVersion", maxVersionPostSQLUpdate)
            .table(target_tbl)
            .where("_change_type in ('update_postimage', 'insert')")
    
          val window = Window
            .partitionBy(prim_key_columns.map(x => col(x)): _*)
            .orderBy(
              col("_commit_version").desc_nulls_last,
              col("update_ts").desc_nulls_last
            )
          temp_df
            .withColumn("dedup_row_num", row_number().over(window))
            .where("dedup_row_num == 1")
            .drop("dedup_row_num")
    
        } else {
          spark.read
            .format("delta")
            .option("readChangeData", true)
            .option("startingVersion", maxVersion)
            .option("endingVersion", maxVersionPostSQLUpdate)
            .table(target_tbl)
            .where("_change_type in ('update_postimage', 'insert')")
        }
      } else if (
        Config.skip_delta_synapse_write_common_dimension == "true" && spark.conf
          .get(
            "new_data_flag"
          ) == "true"
      ) {
        in0
      } else {
        val target_tbl = Config.target_table_db + "." + Config.target_table
        if (
          Config.generate_load_ready_files == "true" && Config.skip_main_table_load_ready_files == "false"
        ) {
          spark.read.table(target_tbl).where("1 == 2")
        } else {
          in0
        }
      }
    
    val out0 = if (Config.remove_partition_cols_in_load_ready != "None") {
      println(
        "dropping partition cols: " + Config.remove_partition_cols_in_load_ready
      )
      val partition_cols_in_delta =
        Config.remove_partition_cols_in_load_ready.split(",").map(x => x.trim())
      temp_out0.drop(partition_cols_in_delta: _*)
    } else {
      temp_out0
    }
    out0
  }

}
