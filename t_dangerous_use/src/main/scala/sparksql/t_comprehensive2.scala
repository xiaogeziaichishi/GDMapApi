package sparksql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author liuguibin
 * @date 2021-01-07 09:29
 *
 */
object t_comprehensive2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      //.master("local")
      .appName("yichun_ContextApp")
      .getOrCreate()
    val mysqlTargetUrl = "jdbc:mysql://10.82.121.70:4000/ods?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT"
    val mysqlConfigfUrl = "jdbc:mysql://10.82.121.70:4000/smart?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT"
    val mysqlProp: Properties = new Properties()
    mysqlProp.setProperty("user", "root")
    mysqlProp.setProperty("password", "Hzgc@123")
    mysqlProp.setProperty("characterEncoding", "utf8")
    mysqlProp.setProperty("useSSL", "false")
    mysqlProp.setProperty("useUnicode", "true")

    val esOptions = Map(
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "10.82.120.107",
      "es.port" -> "9200",
      "es.read.field.as.array.include" -> "arr1, arr2",
      "es.scroll.size" -> "10000",
      "es.input.use.sliced.partitions" -> "false",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "123456"
    )

    val mysqlSchema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("sex", StringType, true),
        StructField("nation", StringType, true),
        StructField("politics", StringType, true),
        StructField("education", StringType, true),
        StructField("birthday", StringType, true),
        StructField("phone", StringType, true),
        StructField("contact", StringType, true),
        StructField("specialty", StringType, true),
        StructField("organization_name", StringType, true),
        StructField("organization_category", StringType, true),
        StructField("level", StringType, true),
        StructField("duties", StringType, true),
        StructField("card_type", StringType, true),
        StructField("card_code", StringType, true)

      )
    )
    val configFrame4: DataFrame = sparkSession
      .read
      .jdbc(mysqlConfigfUrl, "t_config", mysqlProp)
      .select("NAME", "ID")

    val esFrame1: DataFrame = sparkSession
      .read
      .format("es")
      .options(esOptions)
      .load("zz21_vw_preventiontreatmentmember")
    esFrame1.registerTempTable("master")
    //    ZZ21_VW_PREVENTIONTREATMENTMEMBER
    val esFrame2: DataFrame = sparkSession
      .read
      .format("es")
      .options(esOptions)
      .load("zz21_vw_preventiontreatment")
    esFrame2.registerTempTable("slave")
    //    ZZ21_VW_PREVENTIONTREATMENT
    val esFrame3: DataFrame = sparkSession
      .read
      .format("es")
      .options(esOptions)
      .load("zz21_vw_propertydicts")
      //    ZZ21_VW_PROPERTYDICTS
      .select("DISPLAYNAME", "ID")
    esFrame3.registerTempTable("prop")
    //列名
    val sql = " select " +
      " m.NAME name, " +
      " a.DISPLAYNAME sex, " +
      " b.DISPLAYNAME nation , " +
      " c.DISPLAYNAME education, " +
      " m.BIRTHDAY birthday, " +
      " m.MOBILE phone , " +
      " m.HOMEPHONE contact , " +
      " d.DISPLAYNAME specialty , " +
      " g.DISPLAYNAME organization_name, " +
      " e.DISPLAYNAME duties, " +
      " f.DISPLAYNAME card_type ," +
      " m.IDCARDNO card_code " +
      " from master m " +
      " left join prop a on m.GENDER = a.ID " +
      " left join prop b on m.NATION = b.ID " +
      " left join prop c on m.SCHOOLSTR = c.ID  " +
      " left join prop d on m.SPECIALTY = d.ID " +
      " left join prop e on m.UNITJOB = d.ID " +
      " left join prop f on m.CERTIFICATECODE = d.ID " +
      " left join slave s on m.PREVENTIONTREATMENTID = s.ID " +
      " left join prop g on s.NAME = g.ID "

    val halfFrame: DataFrame = sparkSession.sql(sql)

    //先计算前面2张表
    val value2tab: RDD[Row] = halfFrame.rdd.mapPartitions(iter => {
      val list = ArrayBuffer[Row]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val str1: String = Option(row.getAs("name")).getOrElse("")
        val str2: String = Option(row.getAs("sex")).getOrElse("")
        val str3: String = Option(row.getAs("nation")).getOrElse("")
        val str4: String = Option(row.getAs("politics")).getOrElse("")
        val str5: String = Option(row.getAs("education")).getOrElse("")
        val str6: String = Option(row.getAs("birthday")).getOrElse("")
        val str7: String = Option(row.getAs("phone")).getOrElse("")
        val str8: String = Option(row.getAs("contact")).getOrElse("")
        val str9: String = Option(row.getAs("specialty")).getOrElse("")
        val str10: String = Option(row.getAs("organization_name")).getOrElse("")
        val str11 = "群防群治组织"
        val str12 = ""
        val str13: String = Option(row.getAs("duties")).getOrElse("")
        val str14: String = Option(row.getAs("card_type")).getOrElse("")
        val str15: String = Option(row.getAs("card_code")).getOrElse("")
        val schema: GenericRowWithSchema = new GenericRowWithSchema(
          Array(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11, str12, str13, str14, str15), mysqlSchema)
        list.append(schema)
      }
      list.iterator
    })
    val frame2Tab: DataFrame = sparkSession.createDataFrame(value2tab, mysqlSchema)

    frame2Tab.write.mode(SaveMode.Append).jdbc(mysqlTargetUrl, "t_comprehensive_organization_person_bak_2", mysqlProp)
    println("=======================》完成")
  }
}
