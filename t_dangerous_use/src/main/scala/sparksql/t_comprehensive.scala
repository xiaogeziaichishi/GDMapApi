package sparksql

import java.util.Properties

import org.apache.log4j.{Level, Logger}
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
object t_comprehensive {
  Logger.getLogger("org.apache").setLevel(Level.WARN)

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
      /*"es.nodes.wan.only" -> "true",*/
      "es.nodes" -> "192.168.130.211",
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
      .load("zz21_vw_comprehensivemember")
    esFrame1.registerTempTable("master")
    //    ZZ21_VW_COMPREHENSIVEMEMBER

    val esFrame2: DataFrame = sparkSession
      .read
      .format("es")
      .options(esOptions)
      .load("zz21_vw_comprehensive")
    esFrame2.registerTempTable("slave")
    //    ZZ21_VW_COMPREHENSIVE
    val esFrame3: DataFrame = sparkSession
      .read
      .format("es")
      .options(esOptions)
      .load("zz21_vw_propertydicts")

      //    ZZ21_VW_PROPERTYDICTS
      .select("DISPLAYNAME", "ID")
    esFrame3.registerTempTable("prop")
    //列名
    /*val house_cols: Array[String] = esFrame1.columns
    val house_cols1: Array[String] = esFrame2.columns
    val house_cols2: Array[String] = esFrame3.columns
    val house_cols3: Array[String] = configFrame4.columns
    val esMasterFrame: DataFrame = esFrame1.select(house_cols.map(x => esFrame1(x).as("MASTER_" + x)): _*)
    val esSlaveFrame: DataFrame = esFrame2.select(house_cols1.map(x => esFrame2(x).as("SLAVE_" + x)): _*)

    /*val esPropFrame1: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP1_" + x)): _*)
    val esPropFrame2: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP2_" + x)): _*)
    val esPropFrame3: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP3_" + x)): _*)
    val esPropFrame4: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP4_" + x)): _*)
    val esPropFrame5: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP5_" + x)): _*)
    val esPropFrame6: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP6_" + x)): _*)
    val esPropFrame7: DataFrame = esFrame3.select(house_cols2.map(x => esFrame3(x).as("PROP7_" + x)): _*)*/

    val configFrame: DataFrame = configFrame4.select(house_cols3.map(x => configFrame4(x).as("CONF_" + x)): _*)

    //join
    val halfFrame: DataFrame = esMasterFrame
      .join(esFrame3, esMasterFrame("MASTER_GENDER") === esFrame3("ID") and
        esMasterFrame("MASTER_NATION") === esFrame3("ID") and
        esMasterFrame("MASTER_POLITICALBACKGROUND") === esFrame3("ID") and
        esMasterFrame("MASTER_SCHOOLSTR") === esFrame3("ID") and
        esMasterFrame("MASTER_SPECIALTY") === esFrame3("ID") and
        esMasterFrame("MASTER_COMPREHENSIVEID") === esSlaveFrame("ID") and
        esSlaveFrame("ID") === esFrame3("ID") and
        esMasterFrame("MASTER_DUTIES") === esFrame3("ID"),
        "left"
      )*/
    //
    //esMasterFrame.join(esFrame3, esMasterFrame("MASTER_GENDER") === esFrame3("ID"), "left")
    val sql = " select " +
      " m.NAME name, " +
      " a.DISPLAYNAME sex, " +
      " b.DISPLAYNAME nation , " +
      " c.DISPLAYNAME politics , " +
      " d.DISPLAYNAME education, " +
      " m.BIRTHDAY birthday, " +
      " m.MOBILE phone , " +
      " m.HOMEPHONE contact , " +
      " e.DISPLAYNAME specialty , " +
      " g.DISPLAYNAME organization_name, " +
      " f.DISPLAYNAME duties, " +
      " m.IDCARDNO card_code " +
      " from master m " +
      " left join prop a on m.GENDER = a.ID " +
      " left join prop b on m.NATION = b.ID " +
      " left join prop c on m.POLITICALBACKGROUND = c.ID " +
      " left join prop d on m.SCHOOLSTR = d.ID  " +
      " left join prop e on m.SPECIALTY = e.ID " +
      " left join prop f on m.DUTIES = f.ID " +
      " left join slave s on m.COMPREHENSIVEID = s.ID " +
      " left join prop g on s.NAME = g.ID "
    val frame: DataFrame = sparkSession.sql(sql)


    //再次join
    //val resFrame: DataFrame = halfFrame.join(esPropFrame6, halfFrame("SLAVE_NAME") === esPropFrame6("PROP6_ID"), "left")
    //3次关联
    //val resultFrame: DataFrame = resFrame.join(configFrame, resFrame("PROP_DISPLAYNAME") === configFrame("CONF_NAME"),"left")
    //先计算前面2张表
    val value2tab: RDD[Row] = frame.rdd.mapPartitions(iter => {
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
        val str11 = "综治机构"
        val str12 = ""
        val str13: String = Option(row.getAs("duties")).getOrElse("")
        val str14 = "111"
        val str15: String = Option(row.getAs("card_code")).getOrElse("")
        val schema: GenericRowWithSchema = new GenericRowWithSchema(
          Array(str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11, str12, str13, str14, str15), mysqlSchema)
        list.append(schema)
      }
      list.iterator
    })
    val frame2Tab: DataFrame = sparkSession.createDataFrame(value2tab, mysqlSchema)
    //val cols2Tab: Array[String] = frame2Tab.columns
    //2个表
    //val tab2Frame: DataFrame = frame2Tab.select(cols2Tab.map(x => frame2Tab(x).as("TAB2_" + x)): _*)
    //tab2Frame.join(configFrame, tab2Frame("TAB2_DISPLAYNAME") === configFrame("CONF_NAME"), "left")
    frame2Tab.show(3000)
    frame2Tab.write.mode(SaveMode.Append).jdbc(mysqlTargetUrl, "t_comprehensive_organization_person_bak", mysqlProp)
    println("=======================》完成")
  }
}
