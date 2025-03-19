package io.prophecy.pipelines.report_top_customers.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config(var flow_flag: String = "lower") extends ConfigBase
