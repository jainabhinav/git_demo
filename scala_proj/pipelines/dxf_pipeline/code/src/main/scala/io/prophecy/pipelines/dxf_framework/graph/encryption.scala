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

object encryption {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import java.security.MessageDigest
    import java.util
    import javax.crypto.Cipher
    import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
    import org.apache.commons.codec.binary.Hex
    import org.apache.spark.sql.functions._
    
    println("###############################")
    println("#####Step name: encryption#####")
    println("###############################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    var out0 = in0
    if (
      (Config.encrypt_cols != "None" && spark.conf.get(
        "new_data_flag"
      ) == "true") || (Config.encrypt_cols != "None" && Config.generate_only_load_ready_files == "true")
    ) {
    
      try {
    
    //Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    
        // loading db secrets
        val encrypt_key =
          dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_EncKey
          )
    
        val encrypt_iv =
          dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_InitVec
          )
    
        val pii_cols =
          Config.encrypt_cols.split(",").map(x => x.trim().toLowerCase())
        val non_pii_cols =
          (in0.columns.map(x => x.toLowerCase()).toList diff pii_cols).toList
    
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
    
        spark.udf.register("encrypt", encryptUDF)
    
        val str = if (Config.encryption_to_uppercase) {
          pii_cols
            .map(x =>
              s"UPPER(encrypt('$encrypt_key','$encrypt_iv'," + x + ")) as " + x
            )
            .mkString(",")
        } else {
          pii_cols
            .map(x => s"encrypt('$encrypt_key','$encrypt_iv'," + x + ") as " + x)
            .mkString(",")
        }
    
        val all_cols = str + "," + non_pii_cols.mkString(",")
        in0.createOrReplaceTempView(
          s"temp_tbl_enc_${Config.pipeline_name.replace("-", "")}"
        )
    
        out0 = spark
          .sql(
            s"select $all_cols from temp_tbl_enc_${Config.pipeline_name.replace("-", "")}"
          )
          .select(in0.columns.map(x => col(x)): _*)
      } catch {
        case e: Exception => {
          println(
            s"""Process failed while encrypting data"""
          )
    
          if (spark.conf.get("main_table_api_type") == "NON-API") {
            import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
            println(s" Removing lock from: ${Config.sk_table_name_override}")
            dbutils.fs.rm(Config.sk_lock_file_path + Config.sk_table_name_override + ".txt")
          }
          throw e
        }
      }
    } else {
      out0 = in0
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
