package com.aikosolar.bigdata


import java.sql.{PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.conf.DFAlarmJobConfig
import com.aikosolar.bigdata.flink.connectors.jdbc.JdbcSink
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions
import com.aikosolar.bigdata.flink.connectors.jdbc.writter.JdbcWriter
import com.aikosolar.bigdata.flink.job.FLinkKafkaRunner
import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

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
  * --job-name=halmfull \
  * --topic=data-collection-halm-yw  \
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092 \
  * --group.id=halm_full_processyw_717 \
  * --hbase.table=ods:ods_f_eqp_ct_halmyw \
  * --checkpointDataUri=hdfs://172.16.98.85:8020/flink-checkpoint \
  * --reset.strategy=latest \
  * --hbase.writeBufferSize=52428800
  *
  * @author carlc
  */
object DFAlarmJob extends FLinkKafkaRunner[DFAlarmJobConfig] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: DFAlarmJobConfig, rawKafkaSource: DataStream[String]): Unit = {
    val histStream: DataStream[Hist] = rawKafkaSource
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
      .filter(r => r.containsKey("eqpid"))
      .filter(r => r.containsKey("puttime"))
      .filter(r => r.containsKey("status"))
      .filter(r => r.containsKey("alarmtext"))
      .filter(r => r.containsKey("alarmcode"))
      .filter(r => StringUtils.isNotBlank(r.getOrDefault("eqpid", "").toString))
      .filter(r => StringUtils.isNotBlank(r.getOrDefault("puttime", "").toString))
      .map(m => {
        val eqpid = m.get("eqpid").toString
        val putTime = m.get("puttime").toString
        val eqp = eqpid.substring(3)
        val factory = eqpid.substring(0, 1) match {
          case "G" => "1000"
          case "Z" => "2000"
          case "T" => "3000"
          case _ => "Other"
        }
        val eqpName = factory + "-" + eqp
        val alarmText = m.getOrDefault("alarmtext", "").toString
        val serorClear = if (StringUtils.isBlank(alarmText)) null else if (alarmText.toLowerCase().contains("cleared")) "D" else null
        val status = m.getOrDefault("status", "").toString
        val alarmId = m.getOrDefault("alarmcode", "").toString
        (Hist(eqpid, status, status, status, null, putTime, null, null, null), Action(eqpid, putTime, alarmId, status, alarmText, serorClear, "alarm", null, factory), Update(eqpid, status, putTime, "false"))
      }).process(new ProcessFunction[(Hist, Action, Update), Hist] {
      lazy val ActionStream = new OutputTag[Action]("ActionStream")
      lazy val UpdateStream = new OutputTag[Update]("UpdateStream")

      override def processElement(value: (Hist, Action, Update), ctx: ProcessFunction[(Hist, Action, Update), Hist]#Context, out: Collector[Hist]): Unit = {
        out.collect(value._1)
        ctx.output(ActionStream, value._2)
        ctx.output(UpdateStream, value._3)
      }

    })

    val actionStream = histStream.getSideOutput(new OutputTag[Action]("ActionStream")).filter(_.serorclear != "D")
    val updateStream = histStream.getSideOutput(new OutputTag[Update]("UpdateStream"))

    val config: Config = ConfigFactory.load()

    val conf = new JdbcConnectionOptions.Builder()
      .withDriverName(config.getString("connection.drivername"))
      .withUrl(config.getString("connection.url"))
      .withUsername(config.getString("connection.username"))
      .withPassword(config.getString("connection.password"))
      .build()

    //    val conf = new JdbcConnectionOptions("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@172.16.111.29:1521:ORCL", "APIPRO", "APIPRO")
    val histSql =
      """
        |INSERT INTO APIPRO.EAS_EQUIPMENT_STATUS_HIST
        | (NAME, STATUSCODENAME, NEWSTATUSNAME, OLDSTATUSNAME, LASTSTATUSDATE, OLDSTATUSDATE, USETIME, UPDATETIME)
        | VALUES(?,?,?,?,?,?,?,?)
      """.stripMargin

    histStream.addSink(new JdbcSink[Hist](conf, histSql, new JdbcWriter[Hist] {
      override def accept(stmt: PreparedStatement, data: Hist): Unit = {
        val formatter: SimpleDateFormat = if (data.oldstatusdate.contains("-")) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        else if (data.oldstatusdate.contains("/")) new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        else new SimpleDateFormat("yyyyMMddHHmmss")

        stmt.setString(1, data.name)
        stmt.setString(2, data.statuscodename)
        stmt.setString(3, data.newstatusname)
        stmt.setString(4, data.oldstatusname)
        stmt.setString(5, data.laststatusdate)
        stmt.setTimestamp(6, new Timestamp(formatter.parse(data.oldstatusdate).getTime))
        stmt.setObject(7, null) //todo 确认
        stmt.setTimestamp(8, null)
      }
    }))


    val actionSql =
      """
        |INSERT INTO APIPRO.EAS_MACHINEDATA_ACTION
        | (MACHINEID, OCCURTIME, ALARMID, MACHINESTATUS, ALARMTEXT, SERORCLEAR, MESSAGETYPE, UPDATETIME, FACTORY)
        | VALUES(?,?,?,?,?,?,?,?,?)
      """.stripMargin

    actionStream.addSink(new JdbcSink[Action](conf, actionSql, new JdbcWriter[Action] {
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
    }))

    val updateSql = "INSERT INTO APIPRO.EAS_EQUIPMENT_STATUS_UPDATE (NAME,STATUSCODENAME,LASTSTATUSDATE,PRDTIMEFLAG) VALUES (?,?,?,?)"

    updateStream.addSink(new JdbcSink[Update](conf, updateSql, new JdbcWriter[Update] {
      override def accept(stmt: PreparedStatement, data: Update): Unit = {
        val formatter: SimpleDateFormat = if (data.laststatusdate.contains("-")) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        else if (data.laststatusdate.contains("/")) new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        else new SimpleDateFormat("yyyyMMddHHmmss")
        stmt.setString(1, data.name)
        stmt.setString(2, data.statuscodename)
        stmt.setTimestamp(3, new Timestamp(formatter.parse(data.laststatusdate).getTime))
        stmt.setString(4, "false")
      }
    }))
  }

  case class Hist(name: String, statuscodename: String, newstatusname: String, oldstatusname: String, laststatusdate: String, oldstatusdate: String, usetime: String = null, updatetime: String = null, tubrid: String)

  case class Action(machineid: String, occurtime: String, alarmid: String, machinestatus: String, alarmtext: String, serorclear: String, messagetype: String, updatetime: String = null, factory: String)

  case class Update(name: String, statuscodename: String, laststatusdate: String, prdtimeflag: String)

}





