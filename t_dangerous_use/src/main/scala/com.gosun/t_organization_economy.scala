package com.gosun


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

object t_organization_economy {
  //Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //import sparkSession.implicits._
    val sparkSession = SparkSession
      .builder()
      .master("local")
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
    val frame_es = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_neweconomicorganizations")
    //frame_es.createOrReplaceTempView("es")
    //val frame2 = sparkSession.sql("select CHIEF,MOBILENUMBER from es")
    //frame2.show(false)
    //sparkSession.close()
    println("加载ES完成=============================================》")
    val url = "jdbc:mysql://10.82.121.70:4000/smart?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"
    val url1 = "jdbc:mysql://10.82.121.70:4000/region?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"
    val prop: Properties = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "Hzgc@123")
    prop.setProperty("characterEncoding", "utf8")
    prop.setProperty("useSSL", "false")
    prop.setProperty("useUnicode", "true")
    val frame_mysql = sparkSession.read.jdbc(url1, "t_region_vw_code", prop)
    println("===========================================>加载MySQL完成！")
    //定义表结构 13个字段
    val resSchema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("region_code", StringType, true),
        StructField("category", StringType, true),
        StructField("address", StringType, true),
        StructField("member_number", IntegerType, true),
        StructField("principal", StringType, true),
        StructField("principal_telephone", StringType, true),
        StructField("code", StringType, true),
        StructField("annotation_time", StringType, true),
        StructField("destroy_time", StringType, true),
        StructField("url", StringType, true),
        StructField("longitude", DoubleType, true),
        StructField("latitude", DoubleType, true)
      )
    )
    //所有列
    val es_cols = frame_es.columns
    val mysql_cols = frame_mysql.columns
    //新表
    val frame1 = frame_es.select(es_cols.map(x => frame_es(x).as("ES_" + x)): _*)
    val frame = frame_mysql.select(mysql_cols.map(x => frame_mysql(x).as("my_" + x)): _*)
    //关联
    val frame_res = frame1.join(frame, frame1("ES_ORGID") === frame("my_vw_id"), "left")
    val newRdd: RDD[Row] = frame_res.rdd.mapPartitions(iter => {
      val list: ArrayBuffer[Row] = new ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val str1 = row.getAs[String]("ES_NAME")
        val str2 = row.getAs[String]("my_code")
        val str3 = row.getAs[String]("ES_STYLE")
        val str4 = row.getAs[String]("ES_RESIDENCE")
        val str5 = Option(row.getAs[String]("ES_EMPLOYEENUMBER")).getOrElse("0").toString.toInt
        val str6 = row.getAs[String]("ES_CHIEF")
        val str7 = row.getAs[String]("ES_MOBILENUMBERSAFE")
        val str8 = row.getAs[String]("ES_LICENSENUMBER")
        val str9 = row.getAs[String]("ES_VALIDITYSTARTDATE")
        val str10 = row.getAs[String]("ES_VALIDITYENDDATE")
        val str11 = row.getAs[String]("ES_IMGURL")
        val str12 = Option(row.getAs[Double]("my_longitude")).getOrElse("0").toString.toDouble
        val str13 = Option(row.getAs[Double]("my_latitude")).getOrElse("0").toString.toDouble

        val schemaData = new GenericRowWithSchema(Array(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11, str12, str13), resSchema)
        list.append(schemaData)
      }
      list.iterator

    })
    val dataFrame = sparkSession.createDataFrame(newRdd, resSchema)
    println("开始写入===================================================================》")
    dataFrame.show(false)
    dataFrame.write.mode(SaveMode.Append).jdbc(url, "t_organization_economy_copy1", prop)
    println("写入完成===============================================================》")
    sparkSession.close()

  }
}
