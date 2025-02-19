package io.prophecy.pipelines.dq_bug_test.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
