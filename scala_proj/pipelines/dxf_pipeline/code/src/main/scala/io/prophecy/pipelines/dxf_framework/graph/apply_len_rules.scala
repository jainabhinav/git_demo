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

object apply_len_rules {
  def apply(context: Context, in0: DataFrame): (DataFrame, DataFrame) = {
    val spark = context.spark
    val Config = context.config
    // This will check the maximum bytes allowed in the column. It can be customised to provide any rule based on which record
    // needs to be rejected.
    
    println("####################################")
    println("#####Step name: apply_len_rules#####")
    println("####################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    var out1 = in0
    val out0 =
      if (
        Config.length_rules_from_metadata_table == "true" && spark.conf.get(
          "new_data_flag"
        ) == "true" && spark.catalog.tableExists(Config.length_rules_metadata_table)
      ) {
        def check_null(obj: Any): String = {
          val str: String = obj match {
            case null  => ""
            case other => other.toString
          }
          str.toLowerCase
        }
        val df_collect = spark.read
          .table(Config.length_rules_metadata_table)
          .where("lower(table_name) = '" + Config.target_table + "'")
          .select(
            "COLUMN_NAME",
            "DATA_TYPE",
            "CHARACTER_MAXIMUM_LENGTH",
            "NUMERIC_PRECISION",
            "NUMERIC_SCALE"
          )
          .collect()
    
        if (df_collect.length > 0) {
          val length_map = df_collect
            .map(x =>
              x(0).toString.toLowerCase -> Seq(
                check_null(x(1)),
                check_null(x(2)),
                check_null(x(3)),
                check_null(x(4))
              )
            )
            .toMap
    
          val audit_columns = Config.audit_cols.split(",").map(x => x.trim()).toList
          val length_map_keys =
            length_map.keySet.filter(x => !audit_columns.contains(x)).toList
    
          val base_df_columns = in0.columns.map(x => x.toLowerCase())
    
          var case_when = "case "
          length_map_keys.filter(x=> base_df_columns.contains(x.toLowerCase())).foreach { x =>
            val column_meta = length_map(x)
            column_meta(0) match {
              case value
                  if List("int", "smallint", "tinyint", "bigint").contains(value) =>
                case_when =
                  case_when + " when length( cast(" + x + " as " + value.replace(
                    "bigint",
                    "long"
                  ) + ")) > " + column_meta(2) + " then '" + x + "' "
              case value if List("varchar", "nvarchar", "char").contains(value) =>
                case_when =
                  case_when + " when length( cast(" + x + " as " + "string" + ")) > " + column_meta(
                    1
                  ) + " then '" + x + "' "
              case _ => case_when = case_when
            }
          }
          case_when = case_when + " else 'pass' end"
    
          if (!case_when.contains("then")) {
            case_when = "'pass'"
          }
          val length_rules_df = in0.withColumn("reject_record", expr(case_when))
          out1 = length_rules_df
          length_rules_df
            .select(length_rules_df.columns.map { x =>
              if (length_map_keys.contains(x)) {
                val column_meta = length_map(x)
                column_meta(0) match {
                  case value
                      if List("int", "smallint", "tinyint", "bigint")
                        .contains(value) =>
                    when(
                      length(
                        col(x).cast(value.replace("bigint", "long"))
                      ) <= column_meta(2).toInt,
                      col(x).cast(value.replace("bigint", "long"))
                    ).otherwise(null).as(x)
                  case value if List("decimal", "numeric").contains(value) =>
                    col(x)
                      .cast(
                        "decimal(" + column_meta(2) + "," + column_meta(3) + ")"
                      )
                      .as(x)
                  case value
                      if List("varchar", "nvarchar", "char").contains(value) =>
                    when(
                      length(col(x).cast("string")) <= column_meta(1).toInt,
                      col(x).cast("string")
                    ).otherwise(null).as(x)
                  case value
                      if List("datetime", "datetime2", "time").contains(value) =>
                    col(x).cast("string").as(x)
                  case value if List("date").contains(value) =>
                    col(x).cast("string").as(x)
                  case _ => col(x).as(x)
                }
              } else col(x)
            }: _*)
    
        } else if (
          Config.length_rules != "None" && spark.conf.get("new_data_flag") == "true"
        ) {
          in0.withColumn("reject_record", expr(Config.length_rules))
        } else {
          in0.withColumn("reject_record", lit("pass"))
        }
      } else if (
        Config.length_rules != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        in0.withColumn("reject_record", expr(Config.length_rules))
      } else {
        in0.withColumn("reject_record", lit("pass"))
      }
    
    import org.apache.spark.storage.StorageLevel
    if (!out1.columns.contains("reject_record")) {
      out1 = out0
    }
    spark.conf.set("reject_record_count", "0")
    
    if (
      (Config.length_rules != "None" || Config.length_rules_from_metadata_table == "true") && spark.conf
        .get("new_data_flag") == "true"
    ) {
      val df = in0.persist(StorageLevel.DISK_ONLY)
    
      out1 = out1.filter(col("reject_record") =!= lit("pass"))
    
      val reject_record_count = out1.count()
    
      spark.conf.set("reject_record_count", reject_record_count.toString)
    
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
    (out0, out1)
  }

}
