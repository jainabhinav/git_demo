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

object apply_reformat_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import _root_.io.prophecy.libs._
    
    println("#########################################")
    println("#####Step name: apply_reformat_rules#####")
    println("#########################################")
    println(
          "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
        )
    // parse config json of key-value pair where key is target column name and value is expression
    
    val out0 =
      if (
        Config.reformat_rules != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        import scala.collection.mutable.ListBuffer
        import java.text.SimpleDateFormat
        import org.apache.spark.storage.StorageLevel
    
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
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for encrypt_key, decrypt_key, envrypt_iv and decrypt_iv on your cluster to use encrypt decrypt as udf"
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
    
        println("Registered encrypt and decrypt UDFs in reforamt")
    
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
    
        def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }
    
        val col_values = jsonStrToMap(Config.reformat_rules.replace("\n", " "))
    
        var outputList = new ListBuffer[org.apache.spark.sql.Column]()
    
        col_values.foreach { case (key, value) =>
          outputList += expr(
            value
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
          ).as(key)
        }
    
        in0.select(
          (outputList ++ List(
            col("file_name_timestamp"),
            col("source_file_base_path"),
            col("source_file_full_path"),
            col("dxf_src_sys_id")
          )): _*
        )
      } else {
        in0 // if no reformat rules are provided
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
