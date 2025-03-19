package io.prophecy.pipelines.customer_orders.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
