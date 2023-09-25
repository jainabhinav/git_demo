package io.prophecy.pipelines.report_top_customers.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
