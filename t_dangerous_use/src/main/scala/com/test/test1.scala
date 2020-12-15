package com.test

import java.util.Properties

import com.gosun.t_event_info
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class test1 extends Serializable {
  //Logger.getLogger("org").setLevel(Level.ERROR)
  //@transient
  val url2: String = "jdbc:mysql://181.181.0.220:4000/device?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"

  val prop2: Properties = new Properties()
  prop2.setProperty("user", "root")
  prop2.setProperty("password", "Hzgc@123")
  prop2.setProperty("characterEncoding", "utf8")
  prop2.setProperty("useSSL", "false")
  prop2.setProperty("useUnicode", "true")

  val session = SparkSession
    .builder()
    .master("local")
    .config("spark.network.timeout", "1200")
    //.persist(StorageLevel.MEMORY_AND_DISK)
    .appName("SQLContextApp_liu")
    .getOrCreate()

  val mysql_frame: DataFrame = session.read.jdbc(url2, "t_region_vw_code", prop2).persist(StorageLevel.MEMORY_AND_DISK)
  val dataFrame: DataFrame = mysql_frame.select("code", "name")

  def searchFiled(x: String) = {

    //dataFrame.show(5)
    //查询值
    val value: RDD[String] = dataFrame.rdd.mapPartitions(iter => {
      val list: ArrayBuffer[String] = new ArrayBuffer[String]()
      while (iter.hasNext) {
        try {
          val row: Row = iter.next()
          val code1: String = Option(row.getAs[String]("code")).getOrElse("").toString
          val name1: String = Option(row.getAs[String]("name")).getOrElse("").toString


          if (code1.equals(x)) {
            list.append(name1)
          }
        } catch {
          case _ => println("111")
        }
      }
      list.iterator

    })

    value.take(1).apply(0)

  }


}


object t {
  def main(args: Array[String]): Unit = {
    val test1 = new test1()
    val str: String = test1.searchFiled("1360981121217") //1360981115203 兰塘村委会
    println(str)
  }
}
