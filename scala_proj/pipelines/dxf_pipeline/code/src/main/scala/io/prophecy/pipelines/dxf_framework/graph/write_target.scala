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

object write_target {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
    // this component writes output to delta table
    println("#################################")
    println("#####Step name: write_target#####")
    println("#################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    val target_tbl = Config.target_table_db + "." + Config.target_table
    
    val current_ts_val = to_timestamp(
      from_utc_timestamp(current_timestamp(), "America/Chicago").cast("string"),
      "yyyy-MM-dd HH:mm:ss"
    )
    
    val run_id = spark.conf.get("run_id")
    var run_id_for_data = run_id
    if (Config.custom_run_id_suffix != "None") {
      run_id_for_data = run_id.substring(0, 8) + Config.custom_run_id_suffix
    }
    
    val uid_col = Config.get_insert_uid_from_source_df match {
              case true if in0.columns.contains("insert_uid") => col("insert_uid")
              case _ => Config.uid.toLowerCase match {
                case "none"  => {
                  import java.nio.charset.StandardCharsets
                  // Hex string
                  val hexString = "E281A3"
                  // Convert hex string to byte array
                  val byteArray = hexString.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
                  // Decode byte array to UTF-8 string
                  val invisibleSep = new String(byteArray, StandardCharsets.UTF_8)
                  val secret = dbutils.secrets.get(scope = Config.api_scope, key = Config.update_uid_key)
                  val plaintextSecret = secret.replace("", invisibleSep)
                  lit(plaintextSecret)
                }
                case _ => lit(Config.uid)
              }
            }
    
    
    // list of primary key columns
    val isPKSet = spark.conf.getAll.contains("primary_key")
    var prim_key_columns = if (isPKSet) {
      spark.conf
        .get("primary_key")
        .split(",")
        .map(_.trim.toLowerCase)
    } else {
      Config.primary_key
        .split(",")
        .map(_.trim.toLowerCase)
    }
    
    if (Config.merge_logic_including_sk) {
      val isSKSet = spark.conf.getAll.contains("sk_service_col")
      val sk_service_col = if (isSKSet) {
        spark.conf
          .get("sk_service_col")
          .toLowerCase
          .trim
      } else {
        Config.sk_service_col.toLowerCase.trim
      }
      if (sk_service_col != "-" && sk_service_col != "none") {
        prim_key_columns =
          List.concat(List(sk_service_col), prim_key_columns.toList).toArray
      }
    
    }
    
    import scala.util.Try
    // Fetch partition columns from target table
    val partition_cols: List[String] = Try {
      spark
        .sql(s"SHOW PARTITIONS $target_tbl")
        .columns
        .map(_.toLowerCase.trim)
        .toList
    }.getOrElse(Nil)
    
    // Parse explicit partition columns from config
    val partition_column_explicit: List[String] =
      Config.partition_column_explicit.toLowerCase match {
        case "none"  => Nil
        case columns => columns.split(",").map(_.toLowerCase.trim).toList
      }
    
    // Combine explicit partition columns with primary key columns and partition columns, then remove duplicates
    prim_key_columns = (partition_column_explicit ++ (prim_key_columns.intersect(
      partition_cols.map(_.trim.toLowerCase)
    ).toList) ++ prim_key_columns).distinct.toArray
    
    if (spark.conf.get("new_data_flag") == "true") {
      if (spark.catalog.tableExists(target_tbl)) {
    
        import _root_.io.delta.tables._
    
        val hash_cols = Config.hash_cols.split(",").map(x => x.trim())
    
        // list of columns to check for updated values in case of delta merge
    
        val audit_columns =
          Config.audit_cols.split(",").map(x => x.trim()).toList
    
        var dup_check_column =
          ((in0.columns.toList diff audit_columns).toList diff prim_key_columns).toList
        // if (Config.hash_cols != "None") {
        //   dup_check_column = (dup_check_column diff hash_cols).toList
        // }
        if (
          Config.temp_output_flag == "false" && Config.target_write_type == "append" && Config.skip_delta_synapse_write_common_dimension == "false"
        ) {
          println("write_target TYPE: Append")
          in0.write
            .format("delta")
            .option(
              "optimizeWrite",
              true
            ) // https://docs.databricks.com/optimizations/auto-optimize.html
            .option(
              "delta.enableChangeDataFeed",
              true
            ) // https://docs.databricks.com/delta/delta-change-data-feed.html
            .mode("append")
            .saveAsTable(target_tbl)
        } else if (
          Config.temp_output_flag == "false" && Config.target_write_type != "scd2" && Config.target_write_type != "scd0" && Config.skip_delta_synapse_write_common_dimension == "false"
        ) {
          println("write_target TYPE: SCD1")
          println("prim_key_columns: ")
          prim_key_columns.foreach(println)
    
          DeltaTable
            .forName(target_tbl)
            .as("target")
            .merge(
              in0.as("source"),
              prim_key_columns
                .map(x => col("source." + x).eqNullSafe(col("target." + x)))
                .reduce(_ && _)
            ) // merge is done on primary key of table
            .whenMatched(
              !(dup_check_column
                .map(x => col("source." + x).eqNullSafe(col("target." + x)))
                .reduce(_ && _)) && (col("source.rec_stat_cd") <= col("target.rec_stat_cd"))
            ) // update will only happen if any of the underlying column is updated
            .update(
              (List(
                "insert_ts" → col("target.insert_ts"),
                "insert_uid" → col("target.insert_uid"),
                "update_ts" → col("source.insert_ts"),
                "update_uid" → col("source.insert_uid"),
                "run_id" → col("source.run_id"),
                "rec_stat_cd" → col("source.rec_stat_cd")
              ) ++ dup_check_column.map(x => x → col("source." + x))).toMap
            ) // update timestamp and uid columns for audit purpose
            .whenNotMatched()
            .insertAll()
            .execute()
        } else if (
          Config.temp_output_flag == "false" && Config.target_write_type == "scd2" && Config.skip_delta_synapse_write_common_dimension == "false"
        ) {
          println("write_target TYPE: SCD2")
    
          case class SCD2Columns(scd2StartDt: String, scd2EndDt: String)
          val scd2StartDt = Config.scd2_columns
            .split(",")
            .filter(_.contains("startTimestamp"))
            .array(0)
            .split(":")
            .array(1)
            .trim
          println("scd2StartDt: " + scd2StartDt)
          val scd2EndDt = Config.scd2_columns
            .split(",")
            .filter(_.contains("endTimestamp"))
            .array(0)
            .split(":")
            .array(1)
            .trim
          println("scd2EndDt: " + scd2EndDt)
          val SCD2ColumnNames = SCD2Columns(scd2StartDt, scd2EndDt)
    
          val target_df = if (Config.scd2_exclude_source_list == "None") {
            spark.read
              .table(target_tbl)
              .filter(
                to_date(col(SCD2ColumnNames.scd2EndDt)) === lit("9999-12-31")
              )
          } else {
            val scd2_exclude_source_list = Config.scd2_exclude_source_list
              .split(",")
              .map(x => ("'" + x.trim + "'"))
              .mkString(",")
            spark.read
              .table(target_tbl)
              .filter(
                to_date(col(SCD2ColumnNames.scd2EndDt)) === lit("9999-12-31")
              )
              .where(s"src_env_sk not in (${scd2_exclude_source_list})")
          }
    
          def getSCD2PKCols(prefix: String): Seq[Column] = {
            prim_key_columns
              .filterNot(_ == SCD2ColumnNames.scd2StartDt)
              .map(x => col(s"$prefix.$x"))
          }
    
          def buildSCD2PKCondition(prefix: String): Column = {
            concat_ws("-", getSCD2PKCols(prefix): _*)
          }
    
          import org.apache.spark.sql.functions._
          val window = Window
            .partitionBy(
              prim_key_columns
                .filterNot(_ == SCD2ColumnNames.scd2StartDt)
                .map(x => col(x)): _*
            )
            .orderBy(desc(SCD2ColumnNames.scd2StartDt))
          val inp_df =
            in0.withColumn("rn", row_number().over(window)).where("rn=1").drop("rn")
    
          val insForExistRecDFJoin = inp_df
            .as("source")
            .join(
              target_df.as("target"),
              buildSCD2PKCondition("source") === buildSCD2PKCondition("target"),
              "left"
            )
            .filter(
              !dup_check_column
                .map(x => col(s"source.$x").eqNullSafe(col(s"target.$x")))
                .reduce(_ && _)
            )
            .where(
              s"to_date(source.${SCD2ColumnNames.scd2StartDt}) >= to_date(nvl(target.${SCD2ColumnNames.scd2StartDt}, '1900-01-01'))"
            )
    
          val insRecords = insForExistRecDFJoin
            .selectExpr("source.*")
    
          val updRecords = insForExistRecDFJoin
            .where(
              s"source.${SCD2ColumnNames.scd2StartDt} <> target.${SCD2ColumnNames.scd2StartDt}"
            )
            .drop(expr(s"target.${SCD2ColumnNames.scd2EndDt}"))
            .selectExpr(
              "target.*",
              s"cast(source.${SCD2ColumnNames.scd2StartDt} - INTERVAL 1 day as timestamp) as ${SCD2ColumnNames.scd2EndDt}"
            )
            .withColumn("run_id", lit(run_id_for_data))
            .withColumn("insert_ts", current_ts_val)
            .withColumn(
              "insert_uid",
              substring(uid_col, 0, 20)
            ) // audit_cols for updates
    
          val inactiveRecordsDF = target_df
            .as("target")
            .join(
              inp_df.as("source"),
              buildSCD2PKCondition("source") === buildSCD2PKCondition("target"),
              "left_anti"
            )
            .filter(to_date(col(SCD2ColumnNames.scd2EndDt)) === lit("9999-12-31"))
            .withColumn(SCD2ColumnNames.scd2EndDt, current_timestamp)
            .withColumn("run_id", lit(run_id_for_data))
            .withColumn("insert_ts", current_ts_val)
            .withColumn(
              "insert_uid",
              substring(uid_col, 0, 20)
            ) // audit_cols for updates
    
          // new logic
          val inactiveRecordsNormDF = target_df
            .as("target")
            .join(
              insRecords.as("source"),
              buildSCD2PKCondition("source") === buildSCD2PKCondition("target"),
              "inner"
            )
            .filter(
              insRecords.col(SCD2ColumnNames.scd2StartDt) =!= target_df.col(
                SCD2ColumnNames.scd2StartDt
              )
            )
            .drop(expr(s"target.${SCD2ColumnNames.scd2EndDt}"))
            .selectExpr(
              "target.*",
              s"cast(source.${SCD2ColumnNames.scd2StartDt} - INTERVAL 1 day as timestamp) as ${SCD2ColumnNames.scd2EndDt}"
            )
            .withColumn("run_id", lit(run_id_for_data))
            .withColumn("insert_ts", current_ts_val)
            .withColumn(
              "insert_uid",
              substring(uid_col, 0, 20)
            ) // audit_cols for updates
    
          val stagedUpdates = insRecords
            .unionByName(updRecords)
            .unionByName(inactiveRecordsDF)
            .unionByName(inactiveRecordsNormDF)
            .filter(col(Config.sk_service_col).isNotNull)
            .distinct
    
          DeltaTable
            .forName(target_tbl)
            .as("target")
            .merge(
              stagedUpdates.as("source"),
              prim_key_columns
                .map(x => col("source." + x).eqNullSafe(col("target." + x)))
                .reduce(_ && _)
            ) // merge is done on primary key of table
            .whenMatched(
              !(dup_check_column
                .map(x => col("source." + x).eqNullSafe(col("target." + x)))
                .reduce(_ && _)) && (col("source.rec_stat_cd") <= col("target.rec_stat_cd"))
            ) // update will only happen if any of the underlying column is updated
            .update(
              (List(
                "insert_ts" → col("target.insert_ts"),
                "insert_uid" → col("target.insert_uid"),
                "update_ts" → col("source.insert_ts"),
                "update_uid" → col("source.insert_uid"),
                "run_id" → col("source.run_id"),
                "rec_stat_cd" → col("source.rec_stat_cd")
              ) ++ dup_check_column.map(x => x → col("source." + x))).toMap
            ) // update timestamp and uid columns for audit purpose
            .whenNotMatched()
            .insertAll()
            .execute()
    
        } else if (
          Config.temp_output_flag == "false" && Config.target_write_type == "scd0" && Config.skip_delta_synapse_write_common_dimension == "false"
        ) {
          println("write_target TYPE: SCD0")
          DeltaTable
            .forName(target_tbl)
            .as("target")
            .merge(
              in0.as("source"),
              prim_key_columns
                .map(x => col("source." + x).eqNullSafe(col("target." + x)))
                .reduce(_ && _)
            ) // merge is done on primary key of table
            .whenNotMatched()
            .insertAll()
            .execute()
        } else if (Config.temp_output_flag == "true") {
          try {
            spark.sql("truncate table " + target_tbl)
          } catch {
            case e: Exception => {
              spark.sql("drop table IF EXISTS " + target_tbl)
            }
          }
          in0.write
            .format("delta")
            .option(
              "optimizeWrite",
              true
            ) // https://docs.databricks.com/optimizations/auto-optimize.html
            .option(
              "delta.enableChangeDataFeed",
              true
            ) // https://docs.databricks.com/delta/delta-change-data-feed.html
            .mode("overwrite")
            .saveAsTable(target_tbl)
        }
    
      } else { // creation of delta table if it does not exist
        in0.write
          .format("delta")
          .option(
            "optimizeWrite",
            true
          ) // https://docs.databricks.com/optimizations/auto-optimize.html
          .option(
            "delta.enableChangeDataFeed",
            true
          ) // https://docs.databricks.com/delta/delta-change-data-feed.html
          .mode("overwrite")
          .saveAsTable(target_tbl)
      }
    } else {
      if (
        spark.conf.get(
          "new_data_flag"
        ) == "false" && Config.temp_output_flag == "true"
      ) {
        try {
          spark.sql("truncate table " + target_tbl)
        } catch {
          case e: Exception => {
            spark.sql("drop table IF EXISTS " + target_tbl)
          }
        }
      }
    }
    val out0 =
      if (
        Config.skip_delta_synapse_write_common_dimension == "true" && spark.conf
          .get("new_data_flag") == "true"
      ) {
        in0
          .as("source")
          .join(
            spark.read.table(target_tbl).as("target"),
            prim_key_columns
              .map(x => col("source." + x).eqNullSafe(col("target." + x)))
              .reduce(_ && _),
            "left"
          )
          .filter(
            prim_key_columns
              .map(x => col("target." + x).isNull)
              .reduce(_ && _)
          )
          .select("source.*")
      } else {
        spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
      }
    out0
  }

}
