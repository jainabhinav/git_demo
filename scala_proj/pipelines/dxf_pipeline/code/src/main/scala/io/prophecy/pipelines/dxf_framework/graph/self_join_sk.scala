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

object self_join_sk {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#################################")
    println("#####Step name: self_join_sk#####")
    println("#################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    
    val target_tbl = Config.target_table_db + "." + Config.target_table
    val isPKSet = spark.conf.getAll.contains("primary_key")
    println("isPKSet: "+isPKSet)
    
    val prim_key_columns = if (isPKSet) {
      spark.conf
    	.get("primary_key")
    	.split(",")
    	.map(x => x.trim().toLowerCase())
    } else {
      Config.primary_key
    	.split(",")
    	.map(x => x.trim().toLowerCase())
    }
    val isSKSet = spark.conf.getAll.contains("sk_service_col")
    var sk_service_col = if (isSKSet) {
        spark.conf
        .get("sk_service_col").toLowerCase.trim
      } else {
        Config.sk_service_col.toLowerCase.trim
      }
    
    
    val survivorshipCnd = (Config.survivorship_flag && !prim_key_columns
      .contains("src_env_sk"))
    val out0 =
      if (
        (survivorshipCnd || sk_service_col != "none") && spark.catalog
          .tableExists(
            target_tbl
          ) && spark.conf.get("new_data_flag") == "true"
      ) {
    
        import pureconfig._
        import pureconfig.generic.auto._
        import scala.language.implicitConversions
    
        // case class to define source
        case class JoinDef(
            fmt: String,
            src: String,
            joinCond: String,
            valCols: List[String],
            joinType: Option[String] = Some("left_outer"),
            filterCond: Option[String] = Some("true"),
            joinHint: Option[String] = Some("broadcast")
        )
    
        val skColElement =
          if (sk_service_col != "none")
            List(
              "in1." + sk_service_col + " as in1_" + sk_service_col
            )
          else List.empty[String]
          
        val survivorshipElement =
          if (survivorshipCnd) List("in1.src_env_sk as in1_src_env_sk")
          else List.empty[String]
    
        val valColsLst = skColElement ::: survivorshipElement
    
        val skColFinalElement =
          if (sk_service_col != "none") List(col(sk_service_col))
          else List.empty[org.apache.spark.sql.Column]
        val survivorshipFinalElement =
          if (survivorshipCnd) List(col("src_env_sk"))
          else List.empty[org.apache.spark.sql.Column]
        val finalColsLst = skColFinalElement ::: survivorshipFinalElement
    
        val joinDef =
          if (
            (prim_key_columns
              .toSet & Config.encrypt_cols
              .split(",")
              .map(x => x.trim().toLowerCase())
              .toSet).size > 0
          ) {
            val encrypt_columns =
              Config.encrypt_cols.split(",").map(x => x.trim().toLowerCase())
    
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
    
              // Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
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
    
            def encrypt(
                key: String,
                ivString: String,
                plainValue: String
            ): String = {
              if (plainValue != null) {
                val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
                cipher.init(
                  Cipher.ENCRYPT_MODE,
                  keyToSpec(key),
                  getIVSpec(ivString)
                )
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
                cipher.init(
                  Cipher.DECRYPT_MODE,
                  keyToSpec(key),
                  getIVSpec(ivString)
                )
                new String(
                  cipher.doFinal(Hex.decodeHex(encryptedValue.toCharArray()))
                )
              } else {
                null
              }
            }
    
            def keyToSpec(key: String): SecretKeySpec = {
              var keyBytes: Array[Byte] = (key).getBytes("UTF-8")
              // val sha: MessageDigest = MessageDigest.getInstance("MD5")
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
    
            println("Registered encrypt and decrypt UDFs in self_join_sk")
    
            JoinDef(
              fmt = "table",
              src = target_tbl,
              joinCond = prim_key_columns
                .map { x =>
                  if (encrypt_columns.contains(x)) {
                    "in1." + x + " <=> " + "aes_encrypt_udf('" + encrypt_key + "', '" + encrypt_iv + "', " + "trim(in0." + x + "))"
                  } else {
                    "in1." + x + " <=> " + "trim(in0." + x + ")"
                  }
                }
                .mkString(" and "),
              valCols = valColsLst
            )
          } else {
            JoinDef(
              fmt = "table",
              src = target_tbl,
              joinCond = prim_key_columns
                .map(x => "trim(in0." + x + ") <=> " + "in1." + x)
                .mkString(" and "),
              valCols = valColsLst
            )
          }
    
        var res = in0
        val df =
          if (joinDef.fmt == "csv") {
            spark.read
              .format("csv")
              .option("header", true)
              .option("sep", ",")
              .load(joinDef.src)
          } else if (joinDef.fmt == "parquet") {
            spark.read.format("parquet").load(joinDef.src)
          } else {
            spark.read.table(joinDef.src)
          }
        val final_cols =
          List("in0.*") ++ joinDef.valCols
        val join_condition = expr(joinDef.joinCond)
        res = res
          .as("in0")
          .join(
            df.where(joinDef.filterCond.get)
              .select(
                (prim_key_columns
                  .map(x => col(x))
                  .toList ++ finalColsLst
                ): _*
              )
              .as("in1"),
            join_condition,
            joinDef.joinType.get
          )
          .selectExpr(final_cols: _*)
        if (sk_service_col != "none" && Config.sk_service_self_join != "false") {
          res.withColumn(
            sk_service_col,
            coalesce( col("in1_" + sk_service_col), col(sk_service_col))
          )
        } else {
          res
        }
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
