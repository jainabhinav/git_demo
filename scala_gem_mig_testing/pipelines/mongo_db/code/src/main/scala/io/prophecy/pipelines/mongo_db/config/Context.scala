package io.prophecy.pipelines.mongo_db.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
