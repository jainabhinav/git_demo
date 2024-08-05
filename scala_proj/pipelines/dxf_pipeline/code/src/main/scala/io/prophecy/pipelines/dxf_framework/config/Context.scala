package io.prophecy.pipelines.dxf_framework.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
