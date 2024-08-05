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

object create_trigger_file {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import java.text.SimpleDateFormat
    import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}
    import scala.util.Try
    import scala.io.Source
    import scala.sys.process.Process
    import org.apache.hadoop.io.IOUtils
    import java.io._
    import java.time._
    import java.time.format._
    
    println("########################################")
    println("#####Step name: create_trigger_file#####")
    println("########################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    def writeTextFile(filePath: String, filename: String, s: String): Unit = {
    
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val file = new Path(filePath + "/" + filename)
      val dataOutputStream: FSDataOutputStream = fs.create(file)
      val bw: BufferedWriter = new BufferedWriter(
        new OutputStreamWriter(dataOutputStream, "UTF-8")
      )
      bw.write(s)
      bw.close()
    
      val crcPath = new Path(filePath + "/." + filename + ".crc")
      if (Try(fs.exists(crcPath)).isSuccess) {
        fs.delete(crcPath, true)
      }
    }
    
    val out0 =
      if (Config.skip_main_table_load_ready_files == "false") {
        if (
          Config.generate_load_ready_files == "true" || (Config.generate_only_load_ready_files == "true")
        ) {
          try {
            println("Generating trigger file")
    
    //<Dataset Name>|<Transfer type - incr/hist>|<Insert/Merge Parquet file Name>|<Update Parquet file Name>|<Insert file Count>|<Update file Count>|<Create TimeStamp in "YYYYMMDD HH24:MI:SS">
    //ids_common.d_cag|incr|d_cag.merge.000.20221127015930.parquet||5502|159878|20221127 02:00:02
            val run_id = spark.conf.get("run_id")
            val dataset_name =
              Config.trigger_file_content_data_mart_prefix + "." + Config.target_table
            val transfer_type = "incr"
            val insert_count = spark.conf.get("insert_load_ready_count")
            val update_count = spark.conf.get("update_load_ready_count")
            val path =
              Config.single_load_ready_path + "/" + Config.target_table + "/" + run_id
                .substring(0, 4)
    
    //<TABLE_NAME>.<RUN_ID>.trigger
            val fileName = Config.target_table + "." + run_id + ".trigger"
            val ins_parquet_file_name =
              Config.target_table + ".insert.000." + run_id + ".parquet"
            val upd_parquet_file_name =
              Config.target_table + ".update.000." + run_id + ".parquet"
            val current_time = Instant
              .now()
              .atZone(ZoneId.of("America/Chicago"))
    
            val current_time_trigger_file = current_time
              .format(DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss"))
              .toString
    
            val current_time_event_log = current_time
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
              .toString
    
            val trggr_data =
              dataset_name + "|" + transfer_type + "|" + ins_parquet_file_name + "|" + upd_parquet_file_name + "|" + insert_count + "|" + update_count + "|" + current_time_trigger_file
    
            writeTextFile(path, fileName, trggr_data)
    
    // write data to event log
            if (Config.skip_delta_synapse_write_common_dimension == "false") {
              val event_log_df = spark
                .createDataFrame(
                  Seq(
                    (
                      Config.data_mart,
                      Config.pipeline_name,
                      Config.target_table,
                      run_id.substring(0, 4) + "/" + ins_parquet_file_name,
                      current_time_event_log
                    ),
                    (
                      Config.data_mart,
                      Config.pipeline_name,
                      Config.target_table,
                      run_id.substring(0, 4) + "/" + upd_parquet_file_name,
                      current_time_event_log
                    )
                  )
                )
                .toDF(
                  "data_mart",
                  "pipeline_name",
                  "event_table",
                  "event_file",
                  "created_ts"
                )
    
              event_log_df.write
                .format("delta")
                .mode("append")
                .partitionBy("event_table")
                .saveAsTable(Config.event_log_table_name)
            }
    
            spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
          } catch {
            case e: Exception => {
              if (spark.conf.get("main_table_api_type") == "NON-API") {
                val update_sql = spark.conf.get("main_table_max_sk_update_sql")
                println(
                  s"""Process failed while generating trigger file and updating event log. Please create trigger file if it does not exist and create entry in event log.
                Please update max sk manually and remove lock to proceed.
                Update metadata table run this query in metadata db: ${update_sql}
                To remove lock for ${Config.target_table}. Run below command in databricks: 
                dbutils.fs.rm(${Config.sk_lock_file_path}${Config.sk_table_name_override}.txt")
                """
                )
              }
              // dbutils.fs.rm(Config.sk_lock_file_path + Config.target_table + ".txt")
              throw e
            }
          }
        } else {
          in0
        }
      } else {
        in0
      }
    out0
  }

}
