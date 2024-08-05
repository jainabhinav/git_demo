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

object add_audit_cols {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
    // this method adds audit column to the existing dataframe
    println("###################################")
    println("#####Step name: add_audit_cols#####")
    println("###################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val run_id = spark.conf.get("run_id")
    
    var run_id_for_data = run_id
    if (Config.custom_run_id_suffix != "None") {
      run_id_for_data = run_id.substring(0, 8) + Config.custom_run_id_suffix
    }
    
    println("run id for data: " + run_id_for_data)
    
    val current_ts_val = to_timestamp(
      from_utc_timestamp(current_timestamp(), "America/Chicago").cast("string"),
      "yyyy-MM-dd HH:mm:ss"
    )
    val uid_col = Config.get_insert_uid_from_source_df match {
          case true if in0.columns.contains("insert_uid") => col("insert_uid")
          case _ => Config.uid.toLowerCase match {
            case "none"  => {
              import java.nio.charset.StandardCharsets
              // Hex string
              val hexString = "E281A3"
              // Convert hex string to byte array
              val byteArray = hexString.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
              // Decode byte array to UTF-8 string
              val invisibleSep = new String(byteArray, StandardCharsets.UTF_8)
              val secret = dbutils.secrets.get(scope = Config.api_scope, key = Config.update_uid_key)
              val plaintextSecret = secret.replace("", invisibleSep)
              lit(plaintextSecret)
            }
            case _ => lit(Config.uid)
          }
        }
    
    val out0 =
      if (
        Config.disable_audit == "false" && spark.conf.get("new_data_flag") == "true"
      ) {
        try {
          val insert_uid_flg = in0.columns.contains("insert_uid") && Config.get_insert_uid_from_source_df
          var temp_df = insert_uid_flg match {
                case true => {
                  val df = in0
                    .select(
                      col("*"),
                      current_ts_val.as("insert_ts"),
                      current_ts_val.as("update_ts"),
                      lit(null).cast("string").as("update_uid"),
                      lit(run_id_for_data).cast("long").as("run_id"),
                      lit(Config.rec_stat_cd).cast("short").as("rec_stat_cd")
                    )
                    .drop("reject_record")
                  df.withColumn("insert_uid", substring(uid_col, 0, 20))
                }
                case false => in0
                  .select(
                    col("*"),
                    current_ts_val.as("insert_ts"),
                    current_ts_val.as("update_ts"),
                    substring(uid_col, 0, 20).as("insert_uid"),
                    lit(null).cast("string").as("update_uid"),
                    lit(run_id_for_data).cast("long").as("run_id"),
                    lit(Config.rec_stat_cd).cast("short").as("rec_stat_cd")
                  )
                  .drop("reject_record")
              }
          if (in0.columns.contains("src_env_sk")) {
            temp_df = temp_df.withColumn("src_env_sk", col("src_env_sk"))
          } else if (in0.columns.contains("dxf_src_sys_id")) {
            temp_df = temp_df.withColumn("src_env_sk", col("dxf_src_sys_id"))
          } else {
            temp_df = temp_df.withColumn("src_env_sk", lit(Config.src_env_sk))
          }
    
          if (Config.run_id_from_filename == "true") {
            temp_df =
              temp_df.withColumn("run_id", col("file_name_timestamp").cast("long"))
          }
    
          if (Config.add_audit_sec_flag == "true") {
            temp_df = temp_df.withColumn("sec_flg", lit(0).cast("short"))
          }
          temp_df
        } catch {
          case e: Exception => {
            println(
              s"""Process failed while generating audit columns. Removing lock from: ${Config.target_table}"""
            )
    
            if (spark.conf.get("main_table_api_type") == "NON-API"){
              import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
              println(s" Removing lock from: ${Config.sk_table_name_override}")
              dbutils.fs.rm(Config.sk_lock_file_path + Config.sk_table_name_override + ".txt")
            }
            throw e
          }
        }
      } else {
        in0
      }
    
    if(Config.debug_flag) {
      var printDf = out0
      if(Config.debug_filter.toLowerCase() != "none"){
        printDf = printDf.where(Config.debug_filter)
      }  
      if(Config.debug_col_list.toLowerCase() != "none"){
        val print_cols = Config.debug_col_list.split(",").map(x => x.trim())
        printDf.selectExpr(print_cols : _*).show(truncate=false)
      } else {
        printDf.show(truncate=true)
      }
    }
    out0
  }

}
