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

object select_final_cols {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    // parse config json of key-value pair where key is target column name and value is data type
    // select required fields as per target schema
    println("######################################")
    println("#####Step name: select_final_cols#####")
    println("######################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val out0 =
      if (
        Config.final_table_schema != "None" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
        try {
          import org.json4s._
          import org.json4s.jackson.JsonMethods._
          import scala.collection.mutable.ListBuffer
          import spark.sqlContext.implicits._
          import play.api.libs.json._
    
          val json = Json.parse(Config.final_table_schema)
    
          val col_values = json.as[JsObject].keys.toSeq
    
          in0.select(col_values.map { x =>
            if (x.endsWith("_sk") && x != "src_env_sk") {
              when(col(x) === "-9090" || col(x) === "-9191", lit(-1).cast("long")).otherwise(col(x)).as(x)
            } else { col(x) }
          }: _*)
        } catch {
          case e: Exception => {
            println(
              s"""Process failed while selecting final columns"""
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
        in0 // if no target schema is provided
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
