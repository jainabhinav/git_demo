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

object optional_joins {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import java.time._
    import java.time.format._
    
    println("#####S#############################")
    println("#####Step name: optional_joins#####")
    println("#####S#############################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        ) 
    val out0 =
      if (
        (Config.reformat_rules_join != "None" || Config.optional_filter_rules_join != "None") && spark.conf
          .get(
            "new_data_flag"
          ) == "true"
      ) {
        import _root_.io.prophecy.abinitio.ScalaFunctions._
        import _root_.io.prophecy.libs._
        import pureconfig._
        import pureconfig.generic.auto._
        import scala.language.implicitConversions
        import org.apache.spark.storage.StorageLevel
        import scala.collection.mutable.ListBuffer
    
        registerAllUDFs(spark: SparkSession)
    
        var encrypt_key = "Secret not defined"
        var encrypt_iv = "Secret not defined"
        var decrypt_key = "Secret not defined"
        var decrypt_iv = "Secret not defined"
    
        import java.security.MessageDigest
        import java.util
        import javax.crypto.Cipher
        import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
        import org.apache.commons.codec.binary.Hex
        import org.apache.spark.sql.functions._
    
        try {
    
    //Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
          // loading db secrets
          encrypt_key = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_EncKey
          )
    
          encrypt_iv = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_InitVec
          )
    
          decrypt_key = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_EncKey
          )
    
          decrypt_iv = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_InitVec
          )
          println("Found secrets successfully for encrypt decrypt UDFs.")
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for encrypt_key, decrypt_key, envrypt_iv and decrypt_iv on your cluster to use encrypt decrypt functions as udf"
            )
          }
        }
    
        def encrypt(key: String, ivString: String, plainValue: String): String = {
          if (plainValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            var encrypted_str =
              Hex.encodeHexString(cipher.doFinal(plainValue.getBytes("UTF-8")))
            encrypted_str = encrypted_str
            return encrypted_str
          } else {
            null
          }
        }
    
        def decrypt(
            key: String,
            ivString: String,
            encryptedValue: String
        ): String = {
          if (encryptedValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            new String(cipher.doFinal(Hex.decodeHex(encryptedValue.toCharArray())))
          } else {
            null
          }
        }
    
        def keyToSpec(key: String): SecretKeySpec = {
          var keyBytes: Array[Byte] = (key).getBytes("UTF-8")
          keyBytes = Hex.decodeHex(key)
          keyBytes = util.Arrays.copyOf(keyBytes, 16)
          new SecretKeySpec(keyBytes, "AES")
        }
    
        def getIVSpec(IVString: String) = {
          new IvParameterSpec(IVString.getBytes() ++ Array.fill[Byte](16-IVString.length)(0x00.toByte))
    
        }
    
        val encryptUDF = udf(encrypt _)
        val decryptUDF = udf(decrypt _)
        spark.udf.register("aes_encrypt_udf", encryptUDF)
        spark.udf.register("aes_decrypt_udf", decryptUDF)
    
        println("Registered encrypt and decrypt UDFs in joins.")
    
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
    
        // case class to define source
        case class JoinDef(
            fmt: String,
            src: String,
            joinCond: String,
            valCols: List[String],
            joinType: Option[String] = Some("left"),
            filterCond: Option[String] = Some("true"),
            joinHint: Option[String] = None,
            delimiter: Option[String] = Some(",")
        )
    
        case class Joins(
            joins: List[JoinDef]
        )
    
        var joinsConfig = List[JoinDef]()
    
        if (Config.reformat_rules_join != "None") {
          joinsConfig = joinsConfig ++
            ConfigSource
              .string(Config.reformat_rules_join.replace("\n", " "))
              .loadOrThrow[Joins].joins
        }
    
        if (Config.optional_filter_rules_join != "None") {
          joinsConfig = joinsConfig ++
            ConfigSource
              .string(Config.optional_filter_rules_join.replace("\n", " "))
              .loadOrThrow[Joins].joins
        }
    
        var res1 = if (Config.source_join_hint != "None") {
          in0.hint(Config.source_join_hint)
        } else {
          in0
        }
        var res2 = if (Config.source_join_hint != "None") {
          in0.hint(Config.source_join_hint)
        } else {
          in0
        }
        val joinDf = joinsConfig.foreach { joinDef =>
          println("optional_join: " + joinDef.src)
          println(
            Instant
              .now()
              .atZone(ZoneId.of("America/Chicago"))
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
              .toString
          )
          var df =
            if (joinDef.fmt == "csv") {
              spark.read
                .format("csv")
                .option("header", true)
                .option("sep", joinDef.delimiter.getOrElse(","))
                .load(joinDef.src)
            } else if (joinDef.fmt == "parquet") {
              spark.read.format("parquet").load(joinDef.src)
            } else if (joinDef.fmt == "delta") {
              spark.read.format("delta").load(joinDef.src)
            } else if (joinDef.fmt == "query") {
              spark.sql(joinDef.src)
            } else {
              spark.read.table(joinDef.src)
            }
          val final_cols =
            List("in0.*") ++ joinDef.valCols.map(x =>
              x.replace("SSSZ", "SSS")
                .replace(
                  "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss.'), ",
                  "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss'), "
                )
                .replace(
                  "aes_encrypt_udf(",
                  "aes_encrypt_udf('" + encrypt_key + "', '" + encrypt_iv + "', "
                )
                .replace(
                  "aes_decrypt_udf(",
                  "aes_decrypt_udf('" + decrypt_key + "', '" + decrypt_iv + "', "
                )
                .replace(
                  "ff3_encrypt_idwdata(",
                  "ff3_encrypt_idwdata_new('" + ff3_encrypt_key + "', '" + ff3_encrypt_tweak + "', "
                )
            )
          val join_condition = expr(
            joinDef.joinCond
              .replace("SSSZ", "SSS")
              .replace(
                "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss.'), ",
                "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss'), "
              )
              .replace(
                "aes_encrypt_udf(",
                "aes_encrypt_udf('" + encrypt_key + "', '" + encrypt_iv + "', "
              )
              .replace(
                "aes_decrypt_udf(",
                "aes_decrypt_udf('" + decrypt_key + "', '" + decrypt_iv + "', "
              )
              .replace(
                "ff3_encrypt_idwdata(",
                "ff3_encrypt_idwdata_new('" + ff3_encrypt_key + "', '" + ff3_encrypt_tweak + "', "
              )
          )
    
          if (joinDef.filterCond.get.toLowerCase().contains("partition by")) {
            val where_cond = joinDef.filterCond.get
            val window_arr = where_cond.split("==")
            df = df
              .withColumn("window_temp_col", expr(window_arr(0).trim()))
              .where("window_temp_col == " + window_arr(1).trim())
              .drop("window_temp_col")
          } else {
            df = df.where(joinDef.filterCond.get)
          }
          if (joinDef.joinHint != None) {
            res1 = res2
              .as("in0")
              .join(
                df.as("in1")
                  .hint(joinDef.joinHint.get),
                join_condition,
                joinDef.joinType.get
              )
              .selectExpr(final_cols: _*)
          } else {
            res1 = res2
              .as("in0")
              .join(
                df.as("in1"),
                join_condition,
                joinDef.joinType.get
              )
              .selectExpr(final_cols: _*)
    
            if (Config.join_persist_flag == "true") {
              res1 = res1.persist(StorageLevel.DISK_ONLY)
              res1.count()
              res2.unpersist()
            }
            res2 = res1
            if(Config.debug_flag) {
              println(s"For Join with ${joinDef.src}, COUNT = ${res2.count()}")
            }
          }
          println(
            Instant
              .now()
              .atZone(ZoneId.of("America/Chicago"))
              .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
              .toString
          )
    
        }
        val join_output_count = res2.count()
        println("join_output_count: " + join_output_count)
        spark.conf.get("join_output_count", join_output_count.toString)
        val source_filter_out_count = spark.conf.get("source_filter_count")
        println("source_filter_out_count: " + source_filter_out_count)
        if (join_output_count != source_filter_out_count.toLong){
          println("Please check your join conditions / lookup data for duplicates on joining condition. Some join is not happening 1 to 1.")
        }
        res2
      } else {
        in0
      }
    out0
  }

}
