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

object optional_default_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import scala.collection.mutable.ListBuffer
    import spark.sqlContext.implicits._
    
    println("###########################################")
    println("#####Step name: optional_default_rules#####")
    println("###########################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
        
    val out0 =
      if (
        Config.default_source_blank_nulls != "None" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
    
        val default_columns = Config.default_source_blank_nulls
          .split(",")
          .map(x => x.trim())
    
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
        for (col_name <- in0.columns) {
          if (default_columns.contains(col_name)) {
            val expression =
              "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else '-' end"
            outputList += expr(expression).as(col_name)
          } else {
            outputList += col(col_name)
          }
        }
    
        in0.select(outputList: _*)
      } else if (
        Config.default_rules != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }
    
        val col_map = jsonStrToMap(Config.default_rules.replace("\n", " "))
    
        val col_values = col_map.keySet.toList
    
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
        for (col_name <- in0.columns) {
          if (col_values.contains(col_name)) {
            outputList += expr(col_map.get(col_name).get.replace("SSSZ", "SSS"))
              .as(col_name)
          } else {
            outputList += expr(col_name).as(col_name)
          }
        }
    
        in0.select(outputList: _*)
      } else {
        in0 // if no default rules is provided
      }
    out0
  }

}
