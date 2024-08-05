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

object apply_source_default {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#########################################")
    println("#####Step name: apply_source_default#####")
    println("#########################################")
    
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    //defaulting decimal types so that correct joins can happen
    val out0 =
      if (
        spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
    
        var temp_df = in0
    
        // default columns used in joins/lookups
        import pureconfig._
        import pureconfig.generic.auto._
        import scala.language.implicitConversions
    
        case class JoinDef(
            fmt: String,
            src: String,
            joinCond: String,
            valCols: List[String],
            joinType: Option[String] = Some("left"),
            filterCond: Option[String] = Some("true"),
            joinHint: Option[String] = None
        )
    
        case class Joins(
            joins: List[JoinDef]
        )
    
        var joinsConfig =
          ConfigSource
            .string(Config.reformat_rules_join.replace("\n", " "))
            .loadOrThrow[Joins]
    
        import scala.collection.mutable.ListBuffer
        var outputList = new ListBuffer[String]()
        joinsConfig.joins.foreach { joinDef =>
          val cond = joinDef.joinCond
          val regex = """in0\.(\w+)""".r
          val matches = regex.findAllIn(cond)
          val colNames = matches.map(_.split('.')(1)).toList
          outputList ++= colNames
        }
        val lookup_cols = outputList.toSet.toList
        temp_df = temp_df.select(in0.dtypes.map { x =>
          if (lookup_cols.contains(x._1)) {
            if (x._2.toLowerCase().endsWith(",0)")) {
              coalesce(col(x._1), lit(0)).cast("long").as(x._1)
            } else if (x._2.toLowerCase().startsWith("long")) {
              coalesce(col(x._1), lit(0)).cast("long").as(x._1)
            } else if (x._2.toLowerCase().startsWith("int")) {
              coalesce(col(x._1), lit(0)).cast("int").as(x._1)
            } else if (
              x._2
                .toLowerCase()
                .startsWith("decimal") || x._2.toLowerCase().startsWith("double")
            )
              coalesce(col(x._1), lit(0)).cast("double").as(x._1)
            else if (x._2.toLowerCase().startsWith("date")) {
              coalesce(col(x._1), lit("1900-01-01")).cast("date").as(x._1)
            } else if (x._2.toLowerCase().startsWith("timestamp")) {
              coalesce(col(x._1), lit("1900-01-01 00:00:00"))
                .cast("timestamp")
                .as(x._1)
            } else
              when(
                col(x._1).isNull || (col(x._1).cast("string") === lit("")),
                lit("-")
              )
                .otherwise(trim(col(x._1)))
                .as(x._1)
          } else col(x._1)
        }: _*)
    
        // defaulting decimal types so that correct joins can happen
        if (Config.default_decimal_types != "false") {
          temp_df = temp_df.select(in0.dtypes.map { x =>
            if (x._2.toLowerCase().endsWith(",0)")) {
              col(x._1).cast("long").as(x._1)
            } else if (
              x._2
                .toLowerCase()
                .startsWith("decimal") || x._2.toLowerCase().startsWith("double")
            )
              coalesce(
                expr(
                  "format_number(cast(`" + x._1 + "` as decimal(38,15)), '0.###############')"
                )
              ).as(x._1)
            else if (
              x._2
                .toLowerCase()
                .startsWith("timestamp") || x._2.toLowerCase().contains("time")
            ) {
              date_format(col(x._1), "yyyy-MM-dd HH:mm:ss.SSS").as(x._1)
            } else if (
              x._2
                .toLowerCase()
                .startsWith("date")
            ) {
              col(x._1).cast("string").as(x._1)
            } else col(x._1)
          }: _*)
        }
        temp_df
      } else {
        in0
      }
    out0
  }

}
