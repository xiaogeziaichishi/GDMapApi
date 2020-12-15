package com.gosun

import java.util.Properties

import com.gosun.t_event_info.{options, sparkSession}
import com.test.test1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object t_event_info {
  //Logger.getLogger("org").setLevel(Level.ERROR)
  val url = "jdbc:mysql://10.82.120.107:4000/yichun?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"
  val url1 = "jdbc:mysql://10.82.121.70:4000/region?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT"
  val options = Map(
    "es.nodes.wan.only" -> "true",
    "es.nodes" -> "10.82.120.107",
    "es.port" -> "9200",
    "es.read.field.as.array.include" -> "arr1, arr2",
    "es.scroll.size" -> "10000",
    "es.input.use.sliced.partitions" -> "false"
  )
  val prop: Properties = new Properties()
  prop.setProperty("user", "root")
  prop.setProperty("password", "Hzgc@123")
  prop.setProperty("characterEncoding", "utf8")
  prop.setProperty("useSSL", "false")
  prop.setProperty("useUnicode", "true")
  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .config("spark.network.timeout", "1200")
    .appName("SQLContextApp_liu")
    .getOrCreate()

  //主类
  def main(args: Array[String]): Unit = {
    val beg: Long = System.currentTimeMillis()

    //加载索引
    val frame_es = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_issues")
      .persist(StorageLevel.MEMORY_AND_DISK) //zz21_vw_issuehastypes
    val frame_es2 = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_issuehastypes")
      .persist(StorageLevel.MEMORY_AND_DISK)
    println("加载ES完成=============================================》")

    val frame_mysql = sparkSession.read.jdbc(url1, "t_region_vw_code", prop).persist(StorageLevel.MEMORY_AND_DISK)
    println("===========================================>加载MySQL完成！")
    //定义表结构 10个字段
    val resSchema = StructType(
      List(
        StructField("grid_code", StringType, true),
        StructField("grid_name", StringType, true),
        StructField("region_code", StringType, true),
        StructField("region_merger_name", StringType, true),
        StructField("name", StringType, true),
        StructField("category", StringType, true),
        StructField("type", StringType, true),
        StructField("time", StringType, true),
        StructField("address", StringType, true),
        StructField("content", StringType, true)
      )
    )
    //所有列
    val es_cols = frame_es.columns
    val es_clos2: Array[String] = frame_es2.columns
    val mysql_cols = frame_mysql.columns
    //新表
    val frame1 = frame_es.select(es_cols.map(x => frame_es(x).as("ES_" + x)): _*).select("ES_ID", "ES_OCCURORG", "ES_SUBJECT", "ES_OCCURDATE", "ES_OCCURLOCATION", "ES_ISSUECONTENT")
    val frame2 = frame_es2.select(es_clos2.map(x => frame_es2(x).as("ES2_" + x)): _*).select("ES2_ISSUEID", "ES2_ISSUETYPEDOMAINID")
    val frame = frame_mysql.select(mysql_cols.map(x => frame_mysql(x).as("my_" + x)): _*).select("my_code", "my_name", "my_parent_code", "my_vw_id")
    //关联
    val frame_res = frame1.join(frame, frame1("ES_OCCURORG") === frame("my_vw_id"), "left")
      .join(frame2, frame1("ES_ID") === frame2("ES2_ISSUEID"), "left")
    println("=======================================>关联完成")
    val test: test1 = new test1()
    val newRdd: RDD[Row] = frame_res.rdd.mapPartitions(iter => {
      val list: ArrayBuffer[Row] = new ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val str1 = Option(row.getAs[String]("my_code")).getOrElse("").toString
        val str2 = Option(row.getAs[String]("my_name")).getOrElse("").toString
        val str3 = Option(row.getAs[String]("my_parent_code")).getOrElse("").toString

        val str4: String = Option(test.searchFiled(str3)).getOrElse("").toString //查询其他类

        val str5 = Option(row.getAs[String]("ES_SUBJECT")).getOrElse("").toString
        val str_tmp = row.getAs[String]("ES2_ISSUETYPEDOMAINID")
        val str6: String = str_tmp match {
          case "42E533FC54C7CFE9E05319020BAC51E0" => "公共安全"
          case "9761A04B41C41234E05343020BAC0CBD" => "公共安全"
          case "42E533F61F1DCFEBE05319020BAC1F64" => "公共安全"
          case "697D2F1976135DDDE05343020BAC8267" => "矛盾纠纷"
          case _ => ""
        }

        //val str_tmp2 = row.getAs[String]("ES2_ISSUETYPEID") //查询其他类
        val str7 = null

        val str8 = row.getAs[String]("ES_OCCURDATE")
        val str9 = row.getAs[String]("ES_OCCURLOCATION")
        val str10 = row.getAs[String]("ES_ISSUECONTENT")


        val schemaData = new GenericRowWithSchema(Array(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10), resSchema)
        list.append(schemaData)
      }
      list.iterator

    })
    println("=======================================>Rdd生成成功")
    val frame_reslut: DataFrame = sparkSession.createDataFrame(newRdd, resSchema)
    frame_reslut.show()
    println("=======================================>开始写入")
    frame_reslut.write.mode(SaveMode.Append).jdbc(url, "t_event_info_copy1", prop)
    val end: Long = System.currentTimeMillis()
    val time = (end - beg) / 1000
    println("=======================================>写入完成 " + time + "s")

    sparkSession.close()
  }


}
