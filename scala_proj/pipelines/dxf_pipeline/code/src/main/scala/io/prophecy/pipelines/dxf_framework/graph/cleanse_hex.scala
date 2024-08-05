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

object cleanse_hex {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("################################")
    println("#####Step name: cleanse_hex#####")
    println("################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    
    val lower_case_col_df = in0.select(in0.columns.map(x => col(x).as(x.toLowerCase)): _*)
    val out0 =
      if (
        spark.conf.get(
          "new_data_flag"
        ) == "true" && Config.hex_cleanse_pattern != "None"
      ) {
    
        import scala.util.matching.Regex
    
        val stringColumns = lower_case_col_df.schema.fields
          .filter(_.dataType == org.apache.spark.sql.types.StringType)
          .map(_.name)
    
        val pattern = Config.hex_cleanse_pattern
    
        import scala.collection.mutable.ListBuffer
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
        println("Cleaning hex values from string columns")
        lower_case_col_df.columns.foreach { x =>
          if (stringColumns.contains(x)) {
            outputList += regexp_replace(col(x), pattern, Config.hex_replace_value).as(x)
          } else {
            outputList += col(x)
          }
        }
        lower_case_col_df.select(outputList: _*)
      } else {
        lower_case_col_df
      }
    out0
  }

}
