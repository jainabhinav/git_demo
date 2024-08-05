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

object read_source {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    // case class to define source
    import pureconfig._
    import pureconfig.generic.auto._
    import scala.language.implicitConversions
    
    import java.time._
    import java.time.format._
    
    println("################################")
    println("#####Step name: read_source#####")
    println("################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val curr_time = Instant.now().atZone(ZoneId.of(s"${Config.source_zoneId}"))
    val run_id =
      Instant
        .now()
        .atZone(ZoneId.of("America/Chicago"))
        .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
        .toString
    println("run_id: " + run_id)
    spark.conf.set("run_id", run_id)
    
    val load_filter_time =
      curr_time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toString
    val load_filter_date = load_filter_time.substring(0, 10)
    
    println("read source started")
    
    case class SrcDef(
        fmt: String,
        src: String,
        delimiter: Option[String] = Some(",")
    )
    
    // Workaround until pureconfig 0.17.1 hits prod
    case class Sources(
        srcs: List[SrcDef]
    )
    
    // SingleOrList is a special class that accepts either a single SrcDef
    // or a list of SrcDef, and returns a List[SrcDef] either way.
    val sourceConfig =
      ConfigSource
        .string(Config.source_path.replace("\n", " "))
        .loadOrThrow[Sources]
    
    val accumulated_process_flag =
      if (Config.accumulated_process_flag == "true") true else false
    
    // creating source dataframe based on source format and path
    val allDFs = sourceConfig.srcs
      .map(srcDef => {
        val df = if (Config.read_incremental_files_flag == "true") {
    
          import _root_.io.delta.tables._
          import org.apache.spark.sql.functions._
          import spark.implicits._
    
          import scala.util.Try
          println("read_incremental_files_flag is true")
          def allFiles(path: String): List[String] = {
            import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
            try {
              dbutils.fs
                .ls(path)
                .map(file => {
                  // Work around double encoding bug
                  val path = file.path.replace("%25", "%").replace("%25", "%")
                  if (file.isDir) allFiles(path)
                  else List(path)
                })
                .reduce(_ ++ _)
            } catch {
              case e: Exception => {
                val empty_list: List[String] = Nil
                empty_list
              }
            }
          }
    
          val cond_arr = Config.incremental_condition_strategy
            .split(",")
            .map(x => x.trim().toLowerCase())
          val upper_cond = cond_arr(1)
          val lower_cond = cond_arr(0)
    
          var lastLoadTimestamp: String =
            if (srcDef.fmt == "query" || (srcDef.fmt == "table")) {
              if (Config.incremental_table_cutoff_date != None) {
                Config.incremental_table_cutoff_date.toString
              } else {
                "1900-01-01"
              }
            } else {
              "19000101000000"
            }
          if (
            spark.catalog.tableExists(
              Config.incremental_load_metadata_table
            ) && !accumulated_process_flag
          ) {
            val lastLoadTimestamp_values = spark
              .sql(
                "SELECT last_process_timestamp from " + Config.incremental_load_metadata_table + " where pipeline_name = '" + Config.pipeline_name + "' and target = '" + Config.target_table + "'"
              )
              .collect()
            if (lastLoadTimestamp_values.length > 0) {
              lastLoadTimestamp = lastLoadTimestamp_values(0)
                .getString(0)
              if (srcDef.fmt != "table" && srcDef.fmt != "query") {
                if (lastLoadTimestamp == "-") {
                  lastLoadTimestamp = "19000101000000"
                }
              } else {
                if (lastLoadTimestamp == "-") {
                  lastLoadTimestamp = "1900-01-01"
                }
              }
            }
          }
    
          println("lastLoadTimestamp: " + lastLoadTimestamp)
    
          if (srcDef.fmt != "table" && srcDef.fmt != "query") {
            val all_files = allFiles(srcDef.src)
              .filter(x => x.endsWith("." + srcDef.fmt))
    
            val all_file_names = all_files.map(x => x.split("//").last)
            println("all_file_names: ", all_file_names)
    
            var incrementalFiles =
              if (!accumulated_process_flag) {
                all_files.filter { x =>
                  val regex = "(.*)/.*(\\d{4}\\d{2}\\d{2}\\d{2}\\d{2}\\d{2}).*".r
                  val regex(dir, timestamp) = x
                  timestamp.toLong > lastLoadTimestamp.toLong
                }
              } else {
                println("accumulated_process_flag loop")
                if (
                  spark.catalog
                    .tableExists(Config.accumulated_processed_files_table)
                ) {
                  val processed_files = spark.read
                    .table(Config.accumulated_processed_files_table)
                    .where(
                      " pipeline_name = '" + Config.pipeline_name + "' and target = '" + Config.target_table + "'"
                    )
                    .select("processed_file")
                    .collect()
                    .map(x => x.getString(0))
    
                  val new_files = (all_files diff processed_files).toList
                  println("new_files: " + new_files.mkString(","))
                  spark.conf
                    .set("accoumulated_process_files", new_files.mkString(","))
                  new_files
                } else {
                  spark.conf
                    .set("accoumulated_process_files", all_files.mkString(","))
                  all_files
                }
    
              }
    
            if (incrementalFiles.length > 0) {
              if (
                srcDef.fmt == "csv" || srcDef.fmt == "txt" || srcDef.fmt == "dat"
              ) {
                spark.read
                  .format("csv")
                  .option("header", true)
                  .option("sep", srcDef.delimiter.getOrElse(","))
                  .load(incrementalFiles: _*)
              } else {
                if (!Config.enable_read_schema_from_target_table) {
                  spark.read
                    .option("mergeSchema", "true")
                    .format("parquet")
                    .load(incrementalFiles: _*)
                } else {
                  val targetTblSchema =
                    spark.read.table(s"${Config.target_table_db}.${Config.target_table}").schema
    
                  spark.read
                    .schema(targetTblSchema)
                    .format("parquet")
                    .load(incrementalFiles: _*)
                }
              }
            } else {
              null
            }
          } else if (srcDef.fmt == "table") {
    
            if (Config.incremental_table_watermark_col == "None") {
              throw new Exception(
                "Please define incremental_table_strategy and incremental_table_watermark_col in case you want to read " +
                  "data incrementally from table. Alternatively you can put read_incremental_files_flag  as false in case incremetnal " +
                  "read is not requried."
              )
            }
    
            if (Config.incremental_table_strategy == "now") {
              val inc_cond =
                Config.incremental_table_watermark_col + " " + lower_cond + " '" + lastLoadTimestamp +
                  "' and " + Config.incremental_table_watermark_col + " " + upper_cond + " '" + load_filter_time + "'"
              println("incrmental_condition now: " + inc_cond)
              spark.read
                .table(srcDef.src)
                .where(
                  inc_cond
                )
                .withColumn(
                  "file_name_timestamp",
                  expr(Config.incremental_table_watermark_col).cast("string")
                )
            } else if (Config.incremental_table_strategy.startsWith("today")) {
    
              val delta_days =
                if (Config.incremental_table_strategy.contains("-")) {
                  Config.incremental_table_strategy.split("-")(1).trim().toInt
                } else {
                  0
                }
    
              val date_filter = LocalDate
                .parse(load_filter_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                .minusDays(delta_days)
                .toString
    
              var end_ts_filter =
                if (Config.incremental_table_strategy_delta_hours == "None") {
                  date_filter
                } else {
                  date_filter + " " + Config.incremental_table_strategy_delta_hours
                }
    
              val inc_cond =
                Config.incremental_table_watermark_col + " " + lower_cond + " '" + lastLoadTimestamp + "' and " + Config.incremental_table_watermark_col + " " + upper_cond + " '" + end_ts_filter + "'"
              println("incrmental_condition today: " + inc_cond)
              spark.read
                .table(srcDef.src)
                .where(
                  inc_cond
                )
                .withColumn(
                  "file_name_timestamp",
                  expr(Config.incremental_table_watermark_col).cast("string")
                )
            } else {
              throw new Exception(
                "Please define incremental_table_strategy and incremental_table_watermark_col in case you want to read " +
                  "data incrementally from table. Alternatively you can put read_incremental_files_flag  as false in case incremental " +
                  "read is not requried."
              )
    
              null
            }
    
          } else if (srcDef.fmt == "query") {
            if (Config.incremental_table_watermark_col == "None") {
              throw new Exception(
                "Please define incremental_table_strategy and incremental_table_watermark_col in case you want to read " +
                  "data incrementally from query. Alternatively you can put read_incremental_files_flag  as false in case incremental " +
                  "read is not requried."
              )
            }
    
            val loadEndTimestamp = if (Config.incremental_table_strategy == "now") {
              load_filter_time
            } else if (Config.incremental_table_strategy.startsWith("today")) {
              val delta_days =
                if (Config.incremental_table_strategy.contains("-")) {
                  Config.incremental_table_strategy.split("-")(1).trim().toInt
                } else {
                  0
                }
              val date_filter = LocalDate
                .parse(load_filter_date, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                .minusDays(delta_days)
                .toString
              var end_ts_filter =
                if (Config.incremental_table_strategy_delta_hours == "None") {
                  date_filter
                } else {
                  date_filter + " " + Config.incremental_table_strategy_delta_hours
                }
              end_ts_filter
            } else if (Config.incremental_table_strategy.startsWith("ts")) {
              // TODO: Check if Config.incremental_table_strategy is of actual Timestamp form
              val possibleTs =
                Config.incremental_table_strategy.split("ts")(1).trim()
              val dateFormatPattern = "yyyy-MM-dd HH:mm:ss"
              val formatter = DateTimeFormatter.ofPattern(dateFormatPattern)
    
              Try(formatter.parse(possibleTs)) match {
                case scala.util.Success(_) =>
                  // Date parsing successful
                  println(s"$possibleTs is in the correct format.")
                case scala.util.Failure(_) =>
                  // Date parsing failed
                  throw new Exception(
                    s"Failed in read_source. $possibleTs is not in the correct format."
                  )
              }
              possibleTs
            } else {
              throw new Exception(
                "Please define incremental_table_strategy in case you want to read " +
                  "data incrementally from query. Alternatively you can put read_incremental_files_flag  as false in case incremental " +
                  "read is not requried."
              )
              ""
            }
    
            val where_query =
              if (Config.incremental_query_replace_strategy == "query_replace") {
                srcDef.src
                  .replace(Config.start_ts_query_repr, s"'${lastLoadTimestamp}'")
                  .replace(Config.end_ts_query_repr, s"'${loadEndTimestamp}'")
              } else {
    
                val initial_query = srcDef.src.replace(" WHERE ", " where ")
    
                val updated_query = if (!initial_query.contains(" where ")) {
                  initial_query + " where "
                } else {
                  initial_query
                }
    
                val where_index = updated_query.lastIndexOf(" where ")
    
                val clause_to_insert =
                  Config.incremental_table_watermark_col + " " + lower_cond + " '" + lastLoadTimestamp +
                    "' and " + Config.incremental_table_watermark_col + " " + upper_cond + " '" + loadEndTimestamp + "'"
    
                val clause_to_insert_final =
                  if (initial_query.contains(" where ")) {
                    clause_to_insert + " and "
                  } else {
                    clause_to_insert
                  }
                updated_query.substring(
                  0,
                  where_index + 7
                ) + clause_to_insert_final + " " + updated_query.substring(
                  where_index + 7
                )
    
              }
    
            val final_query =
              "select cast(" + Config.incremental_table_watermark_col + " as string) as file_name_timestamp, " + where_query
                .substring(7, where_query.length)
    
            println("final query for reading incremental data: ")
            println(final_query)
            spark.sql(final_query)
          } else {
            null
          }
    
        } else if (
          srcDef.fmt == "csv" || srcDef.fmt == "txt" || srcDef.fmt == "dat"
        ) {
          spark.read
            .format("csv")
            .option("header", true)
            .option("sep", srcDef.delimiter.getOrElse(","))
            .load(srcDef.src)
        } else if (srcDef.fmt == "parquet") {
          try {
            spark.read.format("parquet").load(srcDef.src)
          } catch {
            case e: Exception => {
              null
            }
          }
        } else if (srcDef.fmt == "query") {
          println("query for fetching input data: ")
          println(srcDef.src)
          spark.sql(srcDef.src)
        } else {
          if (Config.read_from_multiple_temp_tables == "true") {
            try {
              spark.read.table(srcDef.src)
            } catch {
              case e: Exception => {
                null
              }
            }
          } else {
            println("Source table used for fetching input data: ")
            println(srcDef.src)
            spark.read.table(srcDef.src)
          }
        }
    
        if (df != null) {
          var output = df
          if (srcDef.fmt != "table" && srcDef.fmt != "query") {
            output = output
              .withColumn(
                "source_file_full_path",
                input_file_name()
              )
              .withColumn(
                "file_name_timestamp",
                regexp_extract(
                  col("source_file_full_path"),
                  "\\d{4}\\d{2}\\d{2}\\d{2}\\d{2}\\d{2}",
                  0
                )
              )
              .withColumn("source_file_base_path", lit(srcDef.src))
    
          } else {
            if (!output.columns.contains("file_name_timestamp")) {
              output = output.withColumn("file_name_timestamp", lit("-"))
            }
            output = output
              .withColumn("source_file_base_path", lit("-"))
              .withColumn("source_file_full_path", lit("-"))
          }
          output
        } else {
          null
        }
    
      })
      .filter(x => x != null)
    
    // Union all input dataframes if more than one is specified
    val out0 = if (allDFs.length > 0) {
      val final_df = allDFs.tail.foldLeft(allDFs.head)((df1, df2) => df1.union(df2))
      if (final_df.rdd.isEmpty) {
        println("read_source: No new data to process. Got empty df.")
        spark.conf.set("new_data_flag", "false")
        spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
      } else {
        spark.conf.set("new_data_flag", "true")
        final_df
      }
    } else {
      println("read_source: No new data to process.")
      spark.conf.set("new_data_flag", "false")
      spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
    }
    out0
  }

}
