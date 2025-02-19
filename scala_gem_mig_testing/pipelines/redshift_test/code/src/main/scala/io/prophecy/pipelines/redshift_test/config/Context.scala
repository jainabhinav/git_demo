package io.prophecy.pipelines.redshift_test.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
