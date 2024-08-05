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

object apply_default_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    // parse config json of key-value pair where key is target column name and value is data type
    println("#####Step name: apply_default_rules#####")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val out0 =
      if (
        Config.final_table_schema != "None" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        import scala.collection.mutable.ListBuffer
        import spark.sqlContext.implicits._
    
        def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }
    
        val audit_columns = Config.audit_cols.split(",").map(x => x.trim()).toList
    
        val col_map = jsonStrToMap(Config.final_table_schema)
    
        val col_values = col_map.keySet.toList
    
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
        val isPKSet = spark.conf.getAll.contains("primary_key")
        val prim_key_columns = if (isPKSet) {
          spark.conf
            .get("primary_key")
            .split(",")
            .map(_.trim.toLowerCase)
        } else {
          Config.primary_key
            .split(",")
            .map(_.trim.toLowerCase)
        }
    
        val isSKSet = spark.conf.getAll.contains("sk_service_col")
        val sk_service_col = if (isSKSet) {
          spark.conf
            .get("sk_service_col")
            .toLowerCase
            .trim
        } else {
          Config.sk_service_col.toLowerCase.trim
        }
    
        for (col_name <- in0.columns) {
          if (prim_key_columns.contains(col_name)) {
            if (col_values.contains(col_name)) {
              val expression = col_map.get(col_name).get match {
                case "string" =>
                  "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else '-' end"
                case "date" =>
                  "coalesce(to_date(trim(" + col_name + "), 'yyyyMMdd'), to_date(trim(" + col_name + "), 'yyyy-MM-dd'), CAST(null as date))"
                case "timestamp" =>
                  "coalesce(to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmssSSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmss'), to_timestamp(rpad("+ col_name +",23,'0'), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(" + col_name + ", 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss'), CAST(null as timestamp))"
                case value if value.startsWith("decimal") => 
                  if (Config.default_to_decimal_type == "true") {
                    "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as " + value + ")"
                  } else {
                    "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
                  }
                case value if List("float", "double").contains(value) =>
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
                case value if value.contains("struct") || value.contains("array") =>
                  col_name
                case _ =>
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as " + col_map
                    .get(col_name)
                    .get + ")"
              }
              outputList += expr(expression).as(col_name)
            } else {
              val expression =
                "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else '-' end"
              outputList += expr(expression).as(col_name)
            }
          } else if (
            audit_columns.contains(
              col_name
            ) || (Config.apply_defaults == "false" && col_values.contains(col_name))
          ) {
            val expression = col_map.get(col_name).get match {
              case "string" =>
                "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else CAST(null as string) end"
              case "date" =>
                "coalesce(to_date(trim(" + col_name + "), 'yyyyMMdd'), to_date(trim(" + col_name + "), 'yyyy-MM-dd'), CAST(null as date))"
              case "timestamp" =>
                "coalesce(to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmss'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss'), CAST(null as timestamp))"
              case value if value.startsWith("decimal") =>
                if (Config.default_to_decimal_type == "true") {
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as " + value + ")"
                } else {
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
                }
              case value if List("float", "double").contains(value) =>
                "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
              case value if value.contains("struct") || value.contains("array") =>
                col_name
              case _ =>
                "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else null end as " + col_map
                  .get(col_name)
                  .get + ")"
            }
            outputList += expr(expression).as(col_name)
          } else if (col_values.contains(col_name) && col_name.endsWith("_dt_sk")) {
            val expression =
              "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else '19000101' end as " + col_map
                .get(col_name)
                .get + ")"
            outputList += expr(expression).as(col_name)
          } else if (col_values.contains(col_name) && col_name == sk_service_col && Config.enable_negative_one_self_join_sk) { 
            println("applying default with -9191 condition...")
            val expression =
              "cast(coalesce(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else '-9191' end, '-9191') as " + col_map
                .get(col_name)
                .get + ")"
            outputList += expr(expression).as(col_name)
          } else if (col_values.contains(col_name) && col_name == sk_service_col) {
            val expression =
              "cast(coalesce(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else '-1' end, '-1') as " + col_map
                .get(col_name)
                .get + ")"
            outputList += expr(expression).as(col_name)
          } else if (col_values.contains(col_name) && col_name.endsWith("_sk")) {
            val expression =
              "cast(coalesce(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else '-9090' end, '-9090') as " + col_map
                .get(col_name)
                .get + ")"
            outputList += expr(expression).as(col_name)
          } else if (col_values.contains(col_name)) {
            val expression = col_map.get(col_name).get match {
              case "string" =>
                "case when lower(" + col_name + ") == 'null' then '-' when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else '-' end"
              case "date" =>
                "coalesce(to_date(trim(" + col_name + "), 'yyyyMMdd'), to_date(trim(" + col_name + "), 'yyyy-MM-dd'), CAST('1900-01-01' as date))"
              case "timestamp" =>
                "coalesce(to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmssSSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmss'), to_timestamp(rpad("+ col_name +",23,'0'), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(" + col_name + ", 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss'), CAST('1900-01-01 00:00:00' as timestamp))"
              case value if value.startsWith("decimal") =>
                if (Config.default_to_decimal_type == "true") {
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST('0' as string) end as " + value + ")"
                } else {
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST('0' as string) end as double)"
                }
              case value if List("float", "double").contains(value) =>
                "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST('0' as string) end as double)"
              case value if value.contains("struct") || value.contains("array") =>
                col_name
              case _ =>
                "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else '0' end as " + col_map
                  .get(col_name)
                  .get + ")"
            }
            outputList += expr(expression).as(col_name)
          } else {
            val expression =
              "coalesce(format_number(cast(" + col_name + " as decimal(38,10)), '0.##########'), " + col_name + ")"
            outputList += expr(col_name).as(col_name)
          }
        }
    
        in0.select(outputList: _*)
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
