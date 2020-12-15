package com.gosun

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

object t_dangerous_use {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .config("spark.network.timeout", "1200")
      .appName("SQLContextApp")
      .getOrCreate()
    val options = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "10.82.120.107",
      "es.port" -> "9200",
      "es.read.field.as.array.include" -> "arr1, arr2",
      "es.scroll.size" -> "10000",
      "es.input.use.sliced.partitions" -> "false"
    )
    //加载索引
    val frame = sparkSession
      .read
      .format("es")
      .options(options)
      .load("ga_t_yhbz_v_rfzxx")
    println("加载ES完成=============================================》")
    frame.show()
    //定义表结构 7个字段
    val resSchema = StructType(
      List(
        StructField("properties", IntegerType, true),
        StructField("issuing_authority", StringType, true),
        StructField("issuing_date", StringType, true),
        StructField("user_unit", StringType, true),
        StructField("begin_use_date", StringType, true),
        StructField("end_use_date", StringType, true),
        StructField("use_permit_num", StringType, true)
      )
    )
    val value: RDD[Row] = frame.rdd.mapPartitions(iter => {
      val list = ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val properties = 4 //1
        val str1: String = row.getAs[String]("FZJG") //2
        //转化date
        //val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
        //val format_date: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //3转化
        val date1: String = row.getAs[String]("FZRQ") //格式20191017000000


        val str2: String = row.getAs[String]("DWMC") //4
        //5转化
        val date2 = row.getAs[String]("RFQSSJ") //5

        val date3 = row.getAs[String]("RFJZSJ") //6

        val str3 = row.getAs[String]("RFZBH") //7

        val schema: GenericRowWithSchema = new GenericRowWithSchema(Array(properties, str1, date1, str2, date2, date3, str3), resSchema)
        list.append(schema)
      }
      list.iterator
    })
    println("取数合并完成=============================================》")
    //创建dataframe
    val dataFrame: DataFrame = sparkSession.createDataFrame(value, resSchema)
    val url = "jdbc:mysql://10.82.121.70:4000/smart?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"
    val prop: Properties = new Properties()

    prop.setProperty("user", "root")
    prop.setProperty("password", "Hzgc@123")
    prop.setProperty("characterEncoding", "utf8")
    prop.setProperty("useSSL", "false")
    prop.setProperty("useUnicode", "true")
    println("开始写入===================================================================》")
    dataFrame.write.mode(SaveMode.Append).jdbc(url, "t_dangerous_use_copy1", prop)
    println("写入完成===============================================================》")
    sparkSession.close()

  }

}
