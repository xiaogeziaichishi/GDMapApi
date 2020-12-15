package com.test

import com.gosun.t_event_info.{options, sparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


object test_es {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.network.timeout", "1200")
      .appName("SQLContextApp")
      .getOrCreate()
    //val path = "C:\\Users\\Raichard\\Desktop\\宜春\\data1\\ZZ_VW_HERESYS\\20200917\\ZZ_VW_HERESYS_1.json"
    /*val fileframe: DataFrame = sparkSession
      .read
      .format("json")
      .load(path)*/

    val options = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "188.188.23.160",
      "es.port" -> "9200",
      "es.read.field.as.array.include" -> "arr1, arr2",
      "es.scroll.size" -> "10000",
      "es.input.use.sliced.partitions" -> "false",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "123456"
    )
    /* fileframe.write.format("org.elasticsearch.spark.sql")
       .options(options)
       .mode(SaveMode.Append)
       .save("zz21_vw_druggys")*/
    sparkSession
      .read
      .format("es")
      .options(options)
      .load("testnlp1130aa").show(false)

    sparkSession.close()

  }


}
