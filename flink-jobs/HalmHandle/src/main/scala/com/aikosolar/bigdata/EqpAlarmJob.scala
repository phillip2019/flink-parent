package com.aikosolar.bigdata

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.conf.AllEqpConfig
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions
import com.aikosolar.bigdata.flink.job.FLinkKafkaRunner
import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._


/**
  *
  * Halm日志数据预处理器
  *
  * input: kafka
  *
  * output: hbase
  *
  * 逻辑: 根据业务逻辑将数据数据处理以后写入hbase中,
  *
  * 运行参数:
  *
  * flink run -m yarn-cluster \
  * -p 3 \
  * -ys 2 \
  * -yjm 1024 \
  * -ytm 2048 \
  * -ynm HalmFull \
  * --class com.aikosolar.bigdata.HalmFullJob  /root/halm/HalmHandle-1.1.0.jar \
  * --job-name=DFAlarmJob
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=df-alarm-group
  * --reset.strategy=latest
  * --hbase.table=xxxx
  * --topic=data-collection-eqp-alarm
  *
  * @author carlc
  */
object EqpAlarmJob extends FLinkKafkaRunner[AllEqpConfig] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: AllEqpConfig, rawKafkaSource: DataStream[String]): Unit = {
    val actionStream: DataStream[Action] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          val key = c.fieldMapping.getOrDefault(en.getKey, en.getKey)
          val value = if (en.getValue == null) "" else en.getValue
          // 统一转换为小写字符串,可以避免很多不必要的麻烦
          result.putIfAbsent(key.toLowerCase(), value)
        }
        result
      })
      .filter(r=>r.containsKey("eqpid"))
      .filter(r=>r.containsKey("puttime"))
      .filter(r=>r.containsKey("status"))
      .filter(r=>r.containsKey("alarmtext"))
      .filter(r=>r.containsKey("alarmcode"))
      .filter(r => StringUtils.isNotBlank(r.getOrDefault("eqpid", "").toString))
      .filter(r => StringUtils.isNotBlank(r.getOrDefault("puttime", "").toString))
      .filter(r=>StringUtils.contains(r.getOrDefault("puttime", "").toString,"cleared")==false )
      .map(m => {
        val eqpid = m.get("eqpid").toString
        val putTime = m.get("puttime").toString
        val factory = eqpid.substring(0, 1) match {
          case "G" => "1000"
          case "Z" => "2000"
          case "T" => "3000"
          case _ => "Other"
        }
        val alarmText = m.getOrDefault("alarmtext", "").toString
        val serorClear =null
        val status = m.getOrDefault("status", "").toString
        val alarmId = m.getOrDefault("alarmcode", "").toString


         Action(eqpid, putTime, alarmId,status,alarmText, serorClear, "alarm", null, factory)
      })


    val config:Config = ConfigFactory.load()
    val conf = new JdbcConnectionOptions(config.getString("connection.drivername"),
      config.getString("connection.url"),config.getString("connection.username"),
      config.getString("connection.password"))


    val actionSql =
      """
        |INSERT INTO APIPRO.EAS_MACHINEDATA_ACTION
        | (MACHINEID, OCCURTIME, ALARMID, MACHINESTATUS, ALARMTEXT, SERORCLEAR, MESSAGETYPE, UPDATETIME, FACTORY)
        | VALUES(?,?,?,?,?,?,?,?,?)
      """.stripMargin

    actionStream.print("EqpAlarm")
   /* actionStream.addSink(new JdbcSink[Action](conf, actionSql, new JdbcWriter[Action] {
      override def accept(stmt: PreparedStatement, data: Action): Unit = {
        stmt.setString(1, data.machineid)
        stmt.setString(2, data.occurtime)
        stmt.setString(3, data.alarmid)
        stmt.setString(4, data.machinestatus)
        stmt.setString(5, data.alarmtext)
        stmt.setString(6, data.serorclear)
        stmt.setString(7, data.messagetype)
        stmt.setString(8, data.updatetime)
        stmt.setString(9, data.factory)
      }
    }))*/


  }

  case class Action(machineid: String, occurtime: String, alarmid: String, machinestatus: String, alarmtext: String, serorclear: String, messagetype: String, updatetime: String = null, factory: String)


}





