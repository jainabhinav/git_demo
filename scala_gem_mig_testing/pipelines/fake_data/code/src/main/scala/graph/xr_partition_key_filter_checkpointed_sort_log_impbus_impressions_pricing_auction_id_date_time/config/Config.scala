package graph.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config() extends ConfigBase
case class Context(spark: SparkSession, config: Config)
