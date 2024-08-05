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

object create_update_load_ready_file {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("##################################################")
    println("#####Step name: create_update_load_ready_file#####")
    println("##################################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    if (Config.skip_main_table_load_ready_files == "false") {
      if (
        Config.generate_load_ready_files == "true" || (Config.generate_only_load_ready_files == "true")
      ) {
        println("Writing update load ready temp file")
        val path =
          Config.load_ready_update_path + "/" + Config.target_table + "/update_files/" + spark.conf
            .get("run_id") + "/"
        in0
          .repartition(1)
          .drop("_change_type", "_commit_version", "_commit_timestamp")
          .write
          .format("parquet")
          .mode("append")
          .save(path)
      }
    }
    val out0 = in0
    out0
  }

}
