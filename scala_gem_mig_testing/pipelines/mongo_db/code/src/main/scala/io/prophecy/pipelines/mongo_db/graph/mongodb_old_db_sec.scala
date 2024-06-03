package io.prophecy.pipelines.mongo_db.graph

import io.prophecy.libs._
import io.prophecy.pipelines.mongo_db.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object mongodb_old_db_sec {

  def apply(context: Context): DataFrame = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    context.spark.read
      .format("mongodb")
      .option(
        "connection.uri",
        f"${"xcv"}://${dbutils.secrets.get(scope = "asd", key = "username")}:${dbutils.secrets
          .get(scope = "asd",                             key = "password")}@${"bnm"}".trim
      )
      .option("database",   "rty")
      .option("collection", "yui")
      .load()
  }

}
