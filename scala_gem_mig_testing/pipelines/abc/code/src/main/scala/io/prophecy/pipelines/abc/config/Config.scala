package io.prophecy.pipelines.abc.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  var asd: SecretValue = SecretValue(providerType = Some("Databricks"),
                                     secretScope = Some("aws"),
                                     secretKey = Some("aws_access_key_id")
  )
) extends ConfigBase
