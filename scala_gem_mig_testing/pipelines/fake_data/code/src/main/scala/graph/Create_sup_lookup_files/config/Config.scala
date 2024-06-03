package graph.Create_sup_lookup_files.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  XR_LOOKUP_DATA:   String = "hdfs:/app_abinitio/dev",
  XR_BUSINESS_DATE: String = "20181225",
  XR_BUSINESS_HOUR: String = "00"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
