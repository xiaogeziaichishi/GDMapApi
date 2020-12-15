package com.test

import java.sql.Timestamp
import java.util.Properties

import org.apache.commons.net.ntp.TimeStamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author liuguibin
 * @date 2020-12-15 09:11
 *
 */
object spark_mysql {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local")
      .config("spark.network.timeout", "1200")
      //.persist(StorageLevel.MEMORY_AND_DISK)
      .appName("SQLContextApp_liu")
      .getOrCreate()

    val url2: String = "jdbc:mysql://181.181.0.220:4000/device?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"

    val prop2: Properties = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "Hzgc@123")
    prop2.setProperty("characterEncoding", "utf8")
    prop2.setProperty("useSSL", "false")
    prop2.setProperty("useUnicode", "true")
    val schema: StructType = StructType(List(
      StructField("addTime", StringType, nullable = true)))

    val frame: DataFrame = session.read.jdbc(url2, "t_sync_fail_device", prop2)
    val dateFrame: DataFrame = frame.select("add_time")
    val value: RDD[Row] = dateFrame.rdd.mapPartitions(iter => {
      val list: ArrayBuffer[Row] = new ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val stamp: String = row.getAs[Timestamp]("add_time").toString
        val schema1 = new GenericRowWithSchema(Array(stamp), schema)
        list.append(schema1)
      }
      list.iterator
    })
     session.createDataFrame(value,schema).show(false)
    session.close()
  }
}
