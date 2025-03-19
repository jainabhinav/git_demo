package io.prophecy.pipelines.test_mig.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
