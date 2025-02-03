package io.prophecy.pipelines.report_top_customers.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    try registerAllUDFs(spark)
    catch {
      case _ => ()
    }
  }

}

object PipelineInitCode extends Serializable
