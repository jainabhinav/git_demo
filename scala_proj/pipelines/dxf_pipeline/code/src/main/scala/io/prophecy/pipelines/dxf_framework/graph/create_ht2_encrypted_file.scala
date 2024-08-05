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

object create_ht2_encrypted_file {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.commons.codec.binary.Hex
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs._
    import org.apache.hadoop.io.IOUtils
    import org.apache.spark.sql.functions._
    import org.apache.spark.storage.StorageLevel
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.write
    import pureconfig._
    import pureconfig.generic.auto._
    import play.api.libs.json._
    
    import java.io.{IOException, _}
    import java.security.MessageDigest
    import java.time._
    import java.time.format._
    import java.util
    import java.sql.DriverManager
    import java.util.Properties
    import javax.crypto.Cipher
    import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
    import scala.collection.mutable.ListBuffer
    import scala.language.implicitConversions
    import scala.util.Try
    import spark.implicits._
    
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
    implicit val formats = DefaultFormats
    
    val out0 =
      if (
        Config.create_ht2_encrypt_file == "true" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {
        println("#####Step name: create ht2_idw encrypted file#####")
        println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
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
        val isSKSet = spark.conf.getAll.contains("sk_service_col")
        val sk_service_col = if (isSKSet) {
          spark.conf
            .get("sk_service_col")
            .toLowerCase
            .trim
        } else {
          Config.sk_service_col.toLowerCase.trim
        }
        val run_id = spark.conf.get("run_id")
        val hadoopConfig = new Configuration()
        val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val load_ready_insert_path =
          Config.ht2_load_ready_insert_path
        val baseFilePath =
          Config.ht2_single_load_ready_path
    
        case class PkSkGen(pks_sks: String, sk_value: String, p_flag: String)
    
        case class SKDef(
            tableName: String,
            skCol: String,
            nkCol: List[String]
        )
    
        case class SKService(
            sks: List[SKDef]
        )
    
        case class PkSKDef(
            fmt: String,
            src: String,
            pkCols: List[String],
            skCols: String,
            synapseTable: Option[String] = None,
            encryptCols: Option[List[String]] = None,
            decryptCols: Option[List[String]] = None,
            dropPartitionCols: Option[List[String]] = None,
            orderByDedup: Option[String] = None
        )
    
        type PkSKDefs = Map[String, PkSKDef]
    
        val pkSKDefConfig =
          ConfigSource.string(Config.pk_sk_info).loadOrThrow[PkSKDefs]
    
        def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }
    
        def copyMerge(
            srcFS: FileSystem,
            srcDir: Path,
            dstFS: FileSystem,
            dstFile: Path,
            deleteSource: Boolean,
            conf: Configuration
        ): Boolean = {
          if (dstFS.exists(dstFile))
            throw new IOException(s"Target $dstFile already exists")
    
          // Source path is expected to be a directory:
          if (srcFS.getFileStatus(srcDir).isDirectory()) {
    
            val outputFile = dstFS.create(dstFile)
            Try {
              srcFS
                .listStatus(srcDir)
                .sortBy(_.getPath.getName)
                .filter(_.getPath.getName.endsWith(".parquet"))
                .collect {
                  case status if status.isFile() =>
                    val inputFile = srcFS.open(status.getPath())
                    Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
                    inputFile.close()
                }
            }
            outputFile.close()
    
            if (deleteSource) srcFS.delete(srcDir, true) else true
          } else false
        }
    
        val json = Json.parse(Config.final_table_schema)
    
        val col_values_from_schema = json.as[JsObject].keys.toSeq.toList
        val audit_columns =
          Config.audit_cols.split(",").map(x => x.trim()).toList
    
        val col_list_wout_audit_column =
          if (Config.ht2_extra_columns_reformat == "true") {
            in0.columns.toList
          } else {
            (col_values_from_schema diff audit_columns)
          }
        val col_map = jsonStrToMap(Config.final_table_schema)
    
        val col_values = col_map.keySet.toList
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
        for (col_name <- in0.columns) {
          if (prim_key_columns.contains(col_name)) {
            if (col_values.contains(col_name)) {
              val expression = col_map.get(col_name).get match {
                case "string" =>
                  "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else '-' end"
                case "date" =>
                  "coalesce(to_date(trim(" + col_name + "), 'yyyyMMdd'), to_date(trim(" + col_name + "), 'yyyy-MM-dd'), CAST(null as date))"
                case "timestamp" =>
                  "coalesce(to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmssSSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmss'), to_timestamp(rpad(" + col_name + ",23,'0'), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(" + col_name + ", 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss.SSS'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss'), CAST(null as timestamp))"
                case value if value.startsWith("decimal") =>
                  "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
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
          } else if (col_values.contains(col_name)) {
            val expression = col_map.get(col_name).get match {
              case "string" =>
                "case when " + col_name + " is not null and trim(" + col_name + ") != '' then trim(" + col_name + ") else CAST(null as string) end"
              case "date" =>
                "coalesce(to_date(trim(" + col_name + "), 'yyyyMMdd'), to_date(trim(" + col_name + "), 'yyyy-MM-dd'), CAST(null as date))"
              case "timestamp" =>
                "coalesce(to_timestamp(trim(" + col_name + "), 'yyyyMMddHHmmss'), to_timestamp(trim(" + col_name + "), 'yyyy-MM-dd HH:mm:ss'), to_timestamp(trim(" + col_name + "), 'yyyyMMdd HH:mm:ss'), CAST(null as timestamp))"
              case value if value.startsWith("decimal") =>
                "cast(case when " + col_name + " is not null then format_number(cast(" + col_name + " as decimal(38,10)), '0.##########') else CAST(null as string) end as double)"
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
          } else {
            val expression =
              "coalesce(format_number(cast(" + col_name + " as decimal(38,10)), '0.##########'), " + col_name + ")"
            outputList += expr(col_name).as(col_name)
          }
        }
    
        val defaulted_df = in0.select(outputList: _*)
    
        var sksConfig =
          ConfigSource.string(Config.optional_sk_config).loadOrThrow[SKService]
    
        sksConfig = SKService(
          List(
            SKDef(
              tableName = Config.target_table,
              skCol = Config.target_table,
              nkCol = col_list_wout_audit_column
            )
          ) ++ sksConfig.sks
        )
    
        var ff3_encrypt_key = "Secret not defined"
        var ff3_encrypt_tweak = "Secret not defined"
    
        try {
    
          // Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
          // loading db secrets
          ff3_encrypt_key = dbutils.secrets.get(
            scope = Config.ff3_encrypt_scope,
            key = Config.ff3_encrypt_key
          )
    
          ff3_encrypt_tweak = dbutils.secrets.get(
            scope = Config.ff3_encrypt_scope,
            key = Config.ff3_encrypt_tweak
          )
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for ff3_encrypt_key and ff3_encrypt_tweak on your cluster to use ff3_encrypt_idwdata as udf"
            )
          }
        }
        val encypt_col_df = spark.read
          .table(Config.ht2_encrypt_columns_meta_table)
          .where("lower(Table_Name) == '" + Config.target_table.toLowerCase + "'")
          .select("fields")
        val encrypt_col_list = if (encypt_col_df.count > 0) {
          encypt_col_df
            .collect()(0)(0)
            .toString
            .split(",")
            .map(x => x.trim())
            .toList
        } else {
          List[String]()
        }
    
        val null_col_df = spark.read
          .table(Config.ht2_nullify_columns_meta_table)
          .where("lower(Table_Name) == '" + Config.target_table.toLowerCase + "'")
          .select("fields")
        val null_cols_list = if (null_col_df.count() > 0) {
          null_col_df
            .collect()(0)(0)
            .toString
            .split(",")
            .map(x => x.trim())
            .toList
        } else {
          List[String]()
        }
    
        println("encrpt_list: " + encrypt_col_list)
        println("null_cols_list: " + null_cols_list)
    
        var finalOutputList = new ListBuffer[org.apache.spark.sql.Column]()
        sksConfig.sks.zipWithIndex.map { ele =>
          val x = ele._1
          val placeholder_table_config = if (x.tableName != Config.target_table) {
            pkSKDefConfig.get(x.tableName).get
          } else {
    
            PkSKDef(
              "",
              "",
              col_list_wout_audit_column,
              Config.target_table
            )
          }
          finalOutputList += struct(
            placeholder_table_config.pkCols.zip(x.nkCol).map { y =>
              if (null_cols_list.contains(y._1)) {
                lit(null).cast("string").as(y._1)
              } else if (encrypt_col_list.contains(y._1)) {
                expr(
                  "ff3_encrypt_idwdata_new('" + ff3_encrypt_key + "', '" + ff3_encrypt_tweak + "', " + y._2 + ")"
                ).as(y._1)
              } else {
                expr(y._2).as(y._1)
              }
            }: _*
          ).as(x.skCol)
    
        }
        val ht2_df = defaulted_df.select(finalOutputList: _*)
    
        println(
          "ht2_idw file write start time: " + Instant
            .now()
            .atZone(ZoneId.of("America/Chicago"))
        )
        val path =
          load_ready_insert_path + "/" + Config.target_table + "/insert_files/" + run_id + "/"
        ht2_df
          .repartition(1)
          .write
          .format("parquet")
          .mode("overwrite")
          .save(path)
    
        println(
          "ht2_idw file single load ready write start time: " + Instant
            .now()
            .atZone(ZoneId.of("America/Chicago"))
        )
        val finalInsertPath = new Path(
          baseFilePath + "/" + Config.target_table + "/encrypt.ht2_idw." + Config.target_table + "." + run_id + ".parquet"
        )
        copyMerge(hdfs, new Path(path), hdfs, finalInsertPath, true, hadoopConfig)
        println(
          "ht2_idw file write end time: " + Instant
            .now()
            .atZone(ZoneId.of("America/Chicago"))
        )
    
        in0
      } else {
        in0
      }
    out0
  }

}
