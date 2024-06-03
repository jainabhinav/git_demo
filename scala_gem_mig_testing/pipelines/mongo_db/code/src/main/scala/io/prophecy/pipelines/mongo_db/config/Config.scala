package io.prophecy.pipelines.mongo_db.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config(var abc: String = "sdfsdg") extends ConfigBase
