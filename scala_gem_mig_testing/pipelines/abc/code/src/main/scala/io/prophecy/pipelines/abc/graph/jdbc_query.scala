package io.prophecy.pipelines.abc.graph

import io.prophecy.libs._
import io.prophecy.pipelines.abc.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object jdbc_query {

  def apply(context: Context): DataFrame = {
    val Config = context.config
    import java.sql._
    var connection: Connection = null
    try {
      DriverManager.getConnection("dsdsaaa",
                                  s"${Config.asd}asdas",
                                  s"${Config.asd}"
      )
      val statement = connection.prepareStatement(
        "DELETE FROM customers WHERE customer_id > 0"
      )
      try statement.executeUpdate()
      finally statement.close()
    } finally if (connection != null) connection.close()
    context.spark.range(1).select(lit(true).as("result"))
  }

}
