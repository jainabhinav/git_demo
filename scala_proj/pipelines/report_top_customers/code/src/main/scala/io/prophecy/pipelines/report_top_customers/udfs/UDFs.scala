package io.prophecy.pipelines.report_top_customers.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("md5_int",                 md5_int)
    spark.udf.register("mbr_id_encrypt",          mbr_id_encrypt)
    spark.udf.register("ff3_encrypt_idwdata_new", ff3_encrypt_idwdata_new)
    registerAllUDFs(spark)
  }

  def md5_int = {
    def md5Int(str: String, littleEndian: Boolean): Long = {
      import java.security.MessageDigest
      val mdDigest = MessageDigest.getInstance("MD5").digest(str.getBytes)
      val buffer   = java.nio.ByteBuffer.wrap(mdDigest)
      if (littleEndian) buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong
      else buffer.getInt
    }
    udf(md5Int _)
  }

  def mbr_id_encrypt = {
    def encryptMemberId(memberId: String): String = {
      val src: Array[Char] = Array.fill(20)('^')
      var (oddSum, evenSum, totSum) = (0, 0, 0)
      for (i <- 0 until math.min(memberId.length, 18)) {
        src(i) = if (memberId.charAt(i) == ' ') '^' else memberId.charAt(i)
        if (Character.isDigit(src(i)))
          if (i % 2 == 0) evenSum += Character.getNumericValue(src(i))
          else oddSum += Character.getNumericValue(src(i))
      }
      totSum = (evenSum % 10 + oddSum % 10) % 10
      for (i <- 0 until math.min(memberId.length, 18)) {
        if (Character.isDigit(src(i)))
          src(i) =
            (48 + (totSum + Character.getNumericValue(src(i))) % 10).toChar
      }
      Seq(src(13),
          src(12),
          src(15),
          src(14),
          src(17),
          src(16),
          src(1),
          src(0),
          src(3),
          src(2),
          src(5),
          src(4),
          src(7),
          src(6),
          src(9),
          src(8),
          src(11),
          src(10)
      ).mkString.replace("^", "").trim()
    }
    udf(encryptMemberId _)
  }

  def ff3_encrypt_idwdata_new = {
    import scala.reflect.runtime.{universe => ru}
    import scala.reflect.runtime.currentMirror
    def ff3_encrypt(
      ff3_key:    String,
      ff3_tweak:  String,
      plainValue: String
    ): String = {
      try {
        if (plainValue != null) {
          val ff3CipherClass = currentMirror.classLoader.loadClass(
            "io.prophecy.cipher.FF3FPECipher"
          )
          val ff3CipherConstructor =
            ff3CipherClass.getConstructor(classOf[String], classOf[String])
          val c = ff3CipherConstructor
            .newInstance(ff3_key, ff3_tweak)
            .asInstanceOf[{
                def encryptPreservingFormat(plainValue: String): String
              }
            ]
          val ciphertext = c.encryptPreservingFormat(plainValue)
          ciphertext
        } else
          null
      } catch {
        case _: ClassNotFoundException =>
          throw new Exception(
            "Please install ff3_fpe_cipher_1_0 and log4j_api_2_17_1 jar on the cluster to use ff3_encrypt_idwdata function"
          )
          null
        case default: Throwable =>
          println("Exception while converting encrypting value: " + plainValue)
          null
      }
    }
    udf(ff3_encrypt _)
  }

}

object PipelineInitCode extends Serializable
