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

object optional_order_by {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("######################################")
    println("#####Step name: optional_order_by#####")
    println("######################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    val out0 = if (Config.orderby_rules != "None" && spark.conf.get("new_data_flag") == "true") {
      import scala.collection.mutable.ListBuffer
      var outputList = new ListBuffer[org.apache.spark.sql.Column]()
      val order_by_rules = Config.orderby_rules.split(',').map(x => x.trim())
    
      order_by_rules.foreach { rule =>
        if (rule.toLowerCase().contains(" asc")) {
          if (rule.toLowerCase().contains(" asc nulls")) {
            if (rule.toLowerCase().contains("nulls first")) {
              outputList += asc_nulls_first(rule.split(" ")(0).trim())
            } else {
              outputList += asc_nulls_last(rule.split(" ")(0).trim())
            }
          } else {
            outputList += asc(rule.split(" ")(0).trim())
    
          }
        } else {
          if (rule.toLowerCase().contains(" desc nulls")) {
            if (rule.toLowerCase().contains("nulls first")) {
              outputList += desc_nulls_first(rule.split(" ")(0).trim())
            } else {
              outputList += desc_nulls_last(rule.split(" ")(0).trim())
            }
          } else {
            outputList += desc(rule.split(" ")(0).trim())
          }
    
        }
      }
    
      in0.orderBy(outputList: _*)
    } else {
      in0
    }
    out0
  }

}
