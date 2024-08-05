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

object decryption {
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
    println("#####Step name: decryption#####")
    println("###############################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    
    var out0 = in0
    if (
      (Config.decrypt_cols != "None" && spark.conf.get(
        "new_data_flag"
      ) == "true" && Config.skip_main_table_load_ready_files == "false") || (Config.decrypt_cols != "None" && Config.generate_only_load_ready_files == "true" && Config.skip_main_table_load_ready_files == "false")
    ) {
    
    //Decryption method for obfuscating PHI / PII fields defined in config encryptColumns
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      println("Decrypting columns")
      // loading db secrets
      val decrypt_key =
        dbutils.secrets.get(
          scope = Config.decrypt_scope,
          key = Config.decrypt_EncKey
        )
    
      val decrypt_iv =
        dbutils.secrets.get(
          scope = Config.decrypt_scope,
          key = Config.decrypt_InitVec
        )
    
      val pii_cols = Config.decrypt_cols.split(",").map(x => x.trim().toLowerCase())
      val non_pii_cols =
        (in0.columns.map(x => x.toLowerCase()).toList diff pii_cols).toList
    
      def decrypt(key: String, ivString: String, encryptedValue: String): String =
        {
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
          new IvParameterSpec(
            IVString.getBytes() ++ Array.fill[Byte](16 - IVString.length)(
              0x00.toByte
            )
          )
        }
    
        val decryptUDF = udf(decrypt _)
    
        spark.udf.register("decrypt", decryptUDF)
    
        val str = if (Config.encryption_to_uppercase) {
          pii_cols
            .map(x =>
              s"decrypt('$decrypt_key','$decrypt_iv'," + x.toUpperCase + ") as " + x
            )
            .mkString(",")
        } else {
          pii_cols
            .map(x => s"decrypt('$decrypt_key','$decrypt_iv'," + x + ") as " + x)
            .mkString(",")
        }
    
        val all_cols = str + "," + non_pii_cols.mkString(",")
        in0.createOrReplaceTempView(
          s"temp_tbl_${Config.pipeline_name.replace("-", "")}"
        )
    
        out0 = spark
          .sql(
            s"select $all_cols from temp_tbl_${Config.pipeline_name.replace("-", "")}"
          )
          .select(in0.columns.map(x => col(x)): _*)
    
    } else {
      out0 = in0
    }
    out0
  }

}
