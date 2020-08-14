package com.aikosolar.bigdata.flink.job

import java.sql.PreparedStatement
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.aikosolar.bigdata.flink.connectors.jdbc.JdbcSink
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions
import com.aikosolar.bigdata.flink.connectors.jdbc.writter.JdbcWriter
import com.aikosolar.bigdata.flink.job.conf.AllEqpConfig
import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.nfa.compiler.NFACompiler
import org.apache.flink.cep.operator.CepOperator
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._


/**
  *
  * 所有设备Alarm 日志数据采集
  *
  * input: kafka  topic: data-collection-eqp-alarm
 * 消息格式
  * {
  * "EqpID": "Z2-DF02",
  * "Status": "",
  * "AlarmText": "User defined (28)",
  * "AlarmCode": "",
  * "PutTime": "2020/07/30 07:29:11",
  * "TubeID1"：状态 0,
  * "TubeID2"：状态 0,
  * "TubeID3"：状态 1,
  * "TubeID4"：状态 0,
  * "TubeID5"：状态 0,
  * }
  * output: oracle
  *
  * 逻辑: 根据业务逻辑将数据数据处理以后写入oracle中,
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
  * --job-name=eqpalarmJob
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=eqp-alarm-group_test
  * --reset.strategy=earliest
  * --hbase.table=ods:ods_f_eqp_all_alarm
  * --topic=data-collection-eqp-alarm
  *
  */
object EqpAlarm2OracleJob extends FLinkKafkaRunner[AllEqpConfig] {
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
          val value = en.getValue
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
      .filter(r => Strings.isValidEqpId(r.get("eqpid")))
      .filter(r=> !StringUtils.contains(Strings.getNotnull(r.getOrDefault("alarmtext", "")),"cleared") )
      .map(m => {
        val eqpid = m.get("eqpid").toString
        val putTime = Strings.getNotnull(m.get("puttime"))
        val factory = eqpid.substring(0, 1) match {
          case "G" => "1000"
          case "Z" => "2000"
          case "T" => "3000"
          case _ => "Other"
        }
        val alarmText = Strings.getNotnull(m.get("alarmtext"))
        val serorClear =null
        val status = m.get("status")
        val alarmId = Strings.getNotnull(m.get("alarmcode"))

         Action(eqpid, putTime, alarmId,status,alarmText, serorClear, "alarm", null, factory)
      })

    val config:Config = ConfigFactory.load()

     val conf = new JdbcConnectionOptions.Builder()
      .withDriverName(config.getString("connection.drivername"))
      .withUrl(config.getString("connection.url"))
      .withUsername(config.getString("connection.username"))
      .withPassword(config.getString("connection.password"))
      .build()
    val actionSql =
      """
        |INSERT INTO APIPRO.EAS_MACHINEDATA_ACTION
        | (MACHINEID, OCCURTIME, ALARMID, MACHINESTATUS, ALARMTEXT, SERORCLEAR, MESSAGETYPE, UPDATETIME, FACTORY)
        | VALUES(?,?,?,?,?,?,?,?,?)
      """.stripMargin
    CepOperator
    NFACompiler
    PatternProcessFunction
//    val pattern:Pattern = null
//    val patternStream:PatternStream = CEP.pattern(actionStream, pattern)

    actionStream.addSink(new JdbcSink[Action](conf, actionSql, new JdbcWriter[Action] {
      override def accept(stmt: PreparedStatement, data: Action): Unit = {
        stmt.setString(1, data.machineid)
        stmt.setString(2, data.occurtime)
        stmt.setString(3, data.alarmid)
        stmt.setObject(4, data.machinestatus)
        stmt.setString(5, data.alarmtext)
        stmt.setString(6, data.serorclear)
        stmt.setString(7, data.messagetype)
        stmt.setString(8, data.updatetime)
        stmt.setString(9, data.factory)

        CEP
      }
    }))


  }

  case class Action(machineid: String, occurtime: String, alarmid: String, machinestatus: AnyRef, alarmtext: String, serorclear: String, messagetype: String, updatetime: String = null, factory: String)


}





