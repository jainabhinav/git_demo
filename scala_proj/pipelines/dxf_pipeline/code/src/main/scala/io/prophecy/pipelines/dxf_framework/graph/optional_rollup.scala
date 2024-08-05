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

object optional_rollup {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("####################################")
    println("#####Step name: optional_rollup#####")
    println("####################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    val out0 = if (Config.rollup_agg_rules != "None" && spark.conf.get("new_data_flag") == "true") {
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      import scala.collection.mutable.ListBuffer
    
      def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }
    
      val groupby_values = jsonStrToMap(Config.rollup_groupby_rules)
      val agg_values = jsonStrToMap(Config.rollup_agg_rules)
    
      var groupby_cols = new ListBuffer[org.apache.spark.sql.Column]()
      var agg_cols =  new ListBuffer[org.apache.spark.sql.Column]()
    
      groupby_values.foreach {case(key, value) => groupby_cols += expr(value).as(key)}
    
      agg_values.foreach {case(key, value) => agg_cols += expr(value).as(key)}
    
      in0.groupBy(groupby_cols:_*).agg(agg_cols.head, agg_cols.tail:_*)
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
