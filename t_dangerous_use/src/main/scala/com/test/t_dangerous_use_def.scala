package com.test

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @author liuguibin
 * @date 2020-12-23 14:03
 *
 */
object t_dangerous_use_def {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("yhj_ContextApp")
      .getOrCreate()
    val resSchema = StructType(
      List(
        /*StructField("name", StringType, true),
        StructField("remark", StringType, true)*/
        StructField("code", StringType, true)
      )
    )
    //mysql t_dangerous_use_2 参数
    val url = "jdbc:mysql://42.192.65.23:4000/webapp?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT"
    val prop: Properties = new Properties()
    prop.setProperty("user", "mysql")
    prop.setProperty("password", "root@123")
    prop.setProperty("characterEncoding", "utf8")
    prop.setProperty("useSSL", "false")
    prop.setProperty("useUnicode", "true")
    val frameAccount: DataFrame = sparkSession.read.jdbc(url, "account", prop)
    //代码表
    val frameAcc: DataFrame = sparkSession.read.jdbc(url, "acc", prop)
    /*val resRdd: RDD[Row] = frameAccount.rdd.map(i => {
      val nameTemp: String = i.getAs[String]("name")
      val code = macthCode(nameTemp, frameAcc)
      val remark: String = i.getAs[String]("remark")
      Row(code, remark)
    })*/

    val resRdd: RDD[Row] = macthCode("刘明慧", frameAcc)
    sparkSession.createDataFrame(resRdd,resSchema).show(false)
  }

  def macthCode(dzstr: String, dataFrame: DataFrame) = {
    var a = "000"
    val resultRdd = dataFrame.rdd.map(i => {
      val code: String = i.getAs[String](1)
      val name: String = i.getAs[String](0)
      if (name.equals(dzstr)) {
        a = code
      }
      Row(a)
    })
    resultRdd
  }

}
