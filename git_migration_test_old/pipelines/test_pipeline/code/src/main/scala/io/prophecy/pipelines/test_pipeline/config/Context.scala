package io.prophecy.pipelines.test_pipeline.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
