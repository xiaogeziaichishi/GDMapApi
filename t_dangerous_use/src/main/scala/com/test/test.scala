package com.test

import java.text.SimpleDateFormat
import java.util.Date

object test {
  def main(args: Array[String]): Unit = {
    val dateformat = new SimpleDateFormat("yyyyMMddHHmmss")
    val dateformat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val str = "202009018000000" //3

    val date1: Date = dateformat.parse(str)

    val date: Date = new Date()
    val pattern: String = dateformat1.toPattern
    println(pattern)
    println(date)
  }
}
