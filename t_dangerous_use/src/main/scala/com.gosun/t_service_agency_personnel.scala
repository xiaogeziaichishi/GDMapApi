package com.gosun

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object t_service_agency_personnel {
  //Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
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
    val baseFrame: DataFrame = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_serviceteammemberbase")
    baseFrame.createOrReplaceTempView("B")
    val detailsFrame: DataFrame = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_serviceteammemberdetails")
    detailsFrame.createOrReplaceTempView("D")
    val teamsFrame = sparkSession
      .read
      .format("es")
      .options(options)
      .load("zz21_vw_serviceteams")
    teamsFrame.createOrReplaceTempView("T")
    println("======================================》加载es全部完成!")
    val resultStructType: StructType = StructType(
      List(
        StructField("name", StringType, true),
        StructField("gender", IntegerType, true),
        StructField("telephone", StringType, true),
        StructField("job", StringType, true),
        StructField("agency_name", StringType, true)
      )
    )
    //所有列
    val bcols: Array[String] = baseFrame.columns
    val dcols: Array[String] = detailsFrame.columns
    val tcols: Array[String] = teamsFrame.columns
    //更改名称
    val bframe = baseFrame.select(bcols.map(x => baseFrame(x).as("B_" + x)): _*)
    val dframe = detailsFrame.select(dcols.map(x => detailsFrame(x).as("D_" + x)): _*)
    val tframe = teamsFrame.select(tcols.map(x => teamsFrame(x).as("T_" + x)): _*)
    //关联
    val dataFrame: DataFrame = dframe.join(bframe, bframe("B_ID") === dframe("D_BASEID"), "inner")
      .join(tframe, dframe("D_TEAMID") === tframe("T_ID"), "inner")
    println("=========================================>关联成功")
    val newValues: RDD[Row] = dataFrame.rdd.mapPartitions(iter => {
      val list: ArrayBuffer[Row] = new ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val str1: String = row.getAs[String]("B_ID")
        val str2: String = row.getAs[String]("B_GENDER")
        val int2: Int = str2 match {
          case "3467459D073A9CC7E053B10213AC774F" => 1
          case "3467459D073B9CC7E053B10213AC774F" => 2
          case "3467459D073C9CC7E053B10213AC774F" => 0
          case "3467459D073D9CC7E053B10213AC774F" => 9
          case _ => 0
        }

        val str3: String = row.getAs[String]("B_MOBILE")
        val str4: String = row.getAs[String]("D_POSITION")
        val str5: String = row.getAs[String]("T_NAME")
        val rowWithSchema = new GenericRowWithSchema(Array(str1, int2, str3, str4, str5), resultStructType)
        list.append(rowWithSchema)
      }
      list.iterator
    })
    val frame: DataFrame = sparkSession.createDataFrame(newValues, resultStructType)
    println("=========================================>开始写入")
    frame.write.mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:mysql://10.82.121.70:4000/smart?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone = GMT")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "t_service_agency_personnel") //表名
      .option("user", "root")
      .option("password", "Hzgc@123")
      .save()
    println("=========================================>写入完成")
    sparkSession.close()
  }
}
