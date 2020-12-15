package com.gosun

import com.crealytics.spark.excel.ExcelDataFrameReader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object pazyz_t_volunteer_es {

  val options = Map(
    "es.nodes.wan.only" -> "true",
    "es.nodes" -> "10.178.77.8",
    "es.port" -> "9200",
    "es.read.field.as.array.include" -> "arr1, arr2",
    "es.scroll.size" -> "10000",
    "es.input.use.sliced.partitions" -> "false"
  )

  val sparkSession = SparkSession
    .builder()
    //.master("local[1]")
    .config("spark.network.timeout", "1200")
    .appName("SQLContextApp_liu")
    .getOrCreate()

  //主类
  def main(args: Array[String]): Unit = {

    val frame: DataFrame = sparkSession.read
      .excel(true)
      .load("file:///srv/test/flume/excel/pazyz_t_volunteer.xlsx")
    frame.show()
    println("excel数据读取成功，开始写入es....")
    frame.write.format("org.elasticsearch.spark.sql")
      .options(options)
      .mode(SaveMode.Append)
      .save("pazyz_t_volunteer")
    println("写入成功！")
    sparkSession.close()
  }
}