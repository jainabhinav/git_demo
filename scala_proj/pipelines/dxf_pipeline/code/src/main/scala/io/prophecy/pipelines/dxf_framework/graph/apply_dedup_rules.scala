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

object apply_dedup_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    // deduplicate on natural keys / adhoc configuration
    println("######################################")
    println("#####Step name: apply_dedup_rules#####")
    println("######################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val out0 =
      if (
        Config.disable_dedup_rules != "true" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
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
    
        var col_values = List[String]()
    
        var col_map = Map[String, String]()
    
        if (Config.final_table_schema != "None") {
    
          import org.json4s._
          import org.json4s.jackson.JsonMethods._
          import scala.collection.mutable.ListBuffer
          import spark.sqlContext.implicits._
    
          def jsonStrToMap(jsonStr: String): Map[String, String] = {
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            parse(jsonStr).extract[Map[String, String]]
          }
    
          col_map = jsonStrToMap(Config.final_table_schema)
    
          col_values = col_map.keySet.toList
    
        }
    
    // default null natural keys column to blank to avoid duplicates
        val new_columns = in0.columns.map { x =>
          if (prim_key_columns.contains(x) && col_values.contains(x)) {
            if ((!x.endsWith("_dt_sk")) && (x.endsWith("_sk"))) {
              when(col(x).isNull, lit("-9090"))
                .otherwise(col(x))
                .cast(col_map.get(x).get)
                .as(x)
            } else if (col_map.get(x).get.startsWith("decimal")) {
              when(col(x).isNull, lit("0"))
                .otherwise(col(x))
                .cast(col_map.get(x).get)
                .as(x)
            } else if (col_map.get(x).get.startsWith("timestamp")) {
              when(
                col(x).isNull,
                lit("1900-01-01 00:00:00")
              )
                .otherwise(col(x))
                .cast(col_map.get(x).get)
                .as(x)
            } else if (col_map.get(x).get.startsWith("date")) {
              when(col(x).isNull, lit("1900-01-01"))
                .otherwise(col(x))
                .cast(col_map.get(x).get)
                .as(x)
            } else if (col_map.get(x).get.startsWith("string")) {
              when(
                col(x).isNull || (col(x).cast("string") === lit("")),
                lit("-")
              )
                .otherwise(trim(col(x)))
                .as(x)
            } else {
              when(col(x).isNull, lit("0").cast(col_map.get(x).get))
                .otherwise(col(x))
                .as(x)
            }
          } else { col(x) }
        }
    
        val temp_df = in0.select(new_columns: _*)
        import org.apache.spark.sql.expressions.Window
        if (Config.dedup_rules != "None") {
          import org.json4s._
          import org.json4s.jackson.JsonMethods._
          import scala.collection.mutable.ListBuffer
    
          def jsonStrToMap(jsonStr: String): Map[String, String] = {
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            parse(jsonStr).extract[Map[String, String]]
          }
    
          val map_values = jsonStrToMap(Config.dedup_rules)
    
          val typeToKeep = map_values.get("dedupType").get
          val groupByColumns =
            map_values.get("dedupColumns").get.split(',').map(x => x.trim()).toList
    
          if (typeToKeep == "first" && map_values.contains("orderByColumns")){
            import scala.collection.mutable.ListBuffer
            println("Deduplicate strategy: ", typeToKeep)
            var outputList = new ListBuffer[org.apache.spark.sql.Column]()
            val order_by_rules = map_values.get("orderByColumns").get.split(',').map(x => x.trim())
    
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
    
            outputList += col("file_name_timestamp").desc_nulls_last
            val window = Window
              .partitionBy(groupByColumns.map(x => col(x)): _*)
              .orderBy(
                outputList: _*
              )
            temp_df
              .withColumn("dedup_row_num", row_number().over(window))
              .where("dedup_row_num == 1")
              .drop("dedup_row_num")
          } else{
            println("Deduplicate strategy: ", typeToKeep)
            temp_df.dropDuplicates(groupByColumns)
          }   
        } else {
          if (prim_key_columns.length > 0 && Config.dedup_columns != "None") {
            import scala.collection.mutable.ListBuffer
            var outputList = new ListBuffer[org.apache.spark.sql.Column]()
            val order_by_rules = Config.dedup_columns.split(',').map(x => x.trim())
    
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
    
            outputList += col("file_name_timestamp").desc_nulls_last
            val window = Window
              .partitionBy(prim_key_columns.map(x => col(x)): _*)
              .orderBy(
                outputList: _*
              )
            temp_df
              .withColumn("dedup_row_num", row_number().over(window))
              .where("dedup_row_num == 1")
              .drop("dedup_row_num")
          } else if (prim_key_columns.length > 0) {
            if (in0.columns.contains("src_env_rnk")) {
              val window = Window
                .partitionBy(prim_key_columns.map(x => col(x)): _*)
                .orderBy(
                  col("src_env_rnk").cast(LongType).asc_nulls_last,
                  col("file_name_timestamp").desc_nulls_last
                )
              temp_df
                .withColumn("dedup_row_num", row_number().over(window))
                .where("dedup_row_num == 1")
                .drop("dedup_row_num")
            } else {
              val window = Window
                .partitionBy(prim_key_columns.map(x => col(x)): _*)
                .orderBy(col("file_name_timestamp").desc_nulls_last)
              temp_df
                .withColumn("dedup_row_num", row_number().over(window))
                .where("dedup_row_num == 1")
                .drop("dedup_row_num")
            }
          } else {
            temp_df
          }
    
        }
      } else {
        in0
      }
    
    if (spark.conf.get("new_data_flag") == "true") {
      val duplicate_filtered_record_count = out0.count().toString
      println("duplicate_filtered_record_count: " + duplicate_filtered_record_count)
      spark.conf.set(
        "duplicate_filtered_record_count",
        duplicate_filtered_record_count
      )
    } else {
      spark.conf.set("duplicate_filtered_record_count", "0")
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
