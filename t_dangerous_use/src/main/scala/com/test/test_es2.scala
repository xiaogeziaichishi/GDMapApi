package com.test

import com.gosun.t_event_info.{options, sparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test_es2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.network.timeout", "1200")
      .appName("SQLContextApp")
      .getOrCreate()
    val options = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "10.82.121.72",
      "es.port" -> "9200",
      "es.read.field.as.array.include" -> "arr1, arr2",
      "es.scroll.size" -> "10000",
      "es.input.use.sliced.partitions" -> "false"
    )
    sparkSession
      .read
      .format("es")
      .options(options)
      .load("yy21_yc_place_information").show()

    sparkSession.close()
  }
}
