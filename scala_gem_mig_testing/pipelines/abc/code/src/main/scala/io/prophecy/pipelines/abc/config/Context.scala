package io.prophecy.pipelines.abc.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
