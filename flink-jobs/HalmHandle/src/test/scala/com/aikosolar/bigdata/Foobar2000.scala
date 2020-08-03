package com.aikosolar.bigdata

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.alibaba.fastjson.JSON

import scala.collection.JavaConversions._

/**
  * json测试 & 字段测试
  *
  * @author carlc
  */
object Foobar2000 {

  def main(args: Array[String]): Unit = {
    val json =
      """
        |{
        |	"EqpID":"Z2-DF02",
        |	"TubeID":"Tube2",
        |	"State":"PRD",
        |	"Tube2Text1":"Move out - lower position",
        |	"PutTime":"2020-07-29 14:09:23",
        |	"RunTime":"4773.0",
        |	"RunCount":"2212.0"
        |}
      """.stripMargin

    /* 字段映射 */
    var fieldMapping: Map[String, String] = Map()
    fieldMapping += ("Tube2Text1" -> "text1")
    fieldMapping += ("EqpID" -> "eqp_id")
    fieldMapping += ("TubeID" -> "tube_id")

    var result: Map[String, Any] = Map()
    val jsonObj = JSON.parseObject(json)
    for (en <- jsonObj.entrySet) {
      val key = en.getKey
      val value = en.getValue

      result += (fieldMapping.getOrElse(key, Strings.humpToUnderLowerString(key)) -> value)
    }

    for (en <- result.entrySet) {
      val key = en.getKey
      val value = en.getValue
      println(s"$key=$value")
    }
  }
}
