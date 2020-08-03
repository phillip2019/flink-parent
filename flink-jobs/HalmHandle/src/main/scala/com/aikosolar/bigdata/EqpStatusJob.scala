package com.aikosolar.bigdata

import java.sql.{PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.conf.AllEqpConfig
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
  * "EqpID": "Z2-TAH08",		设备编号
  * "NewStatus": "SBY",		新状态
  * "NewTime": "2020-07-30 08:51:58",	新时间
  * "OldStatus": "PRD",		老状态
  * "OldTime": "2020-07-30 08:51:58"	老状态时间
  *
  * output: oracle
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
 * --group.id=df-status-group
 * --reset.strategy=latest
 * --hbase.table=xxxx
 * --topic=data-collection-eqp-status
 *
  * @author carlc
  */
object EqpStatusJob extends FLinkKafkaRunner[AllEqpConfig] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: AllEqpConfig, rawKafkaSource: DataStream[String]): Unit = {
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
      .filter(r=>r.containsKey("eqpid"))
      .filter(r=>r.containsKey("newstatus"))
      .filter(r=>r.containsKey("oldstatus"))
      .filter(r=>r.containsKey("newtime"))
      .filter(r=>r.containsKey("oldtime"))
      .filter(r => StringUtils.isNotBlank(r.getOrDefault("eqpid", "").toString))
      .map(m => {

        val eqpid = m.get("eqpid").toString
        val newstatus = m.getOrDefault("newstatus", "").toString
        val oldstatus = m.getOrDefault("oldstatus", "").toString
        val newtime = m.getOrDefault("newtime", "").toString
        val oldtime = m.getOrDefault("oldtime", "").toString



        val eqp=eqpid.substring(3)

        var tubeid=""
       /*
       val status = m.getOrDefault("status", "").toString
       if(eqp.toUpperCase().startsWith("DF") || eqp.toUpperCase().startsWith("PE") || eqp.toUpperCase().startsWith("PR")){
          val tubeidmap: Map[String, String] = new HashMap[String, String]()
          val tubeid1=m.getOrDefault("tubeid1", "").toString
          val tubeid2=m.getOrDefault("tubeid2", "").toString
          val tubeid3=m.getOrDefault("tubeid3", "").toString
          val tubeid4=m.getOrDefault("tubeid4", "").toString
          val tubeid5=m.getOrDefault("tubeid5", "").toString

          tubeid=status match {
            case status if(status.trim.equals(tubeid1)) => "TubeID1"
            case status if(status.trim.equals(tubeid2)) => "TubeID2"
            case status if(status.trim.equals(tubeid3)) => "TubeID3"
            case status if(status.trim.equals(tubeid4)) => "TubeID4"
            case status if(status.trim.equals(tubeid5)) => "TubeID5"
            case _ => ""
          }

        }*/


        (Hist(eqpid, newstatus, newstatus, oldstatus, newtime, oldtime, null, null, tubeid), Update(eqpid, newstatus, newtime, "false"))

      }).process(new ProcessFunction[(Hist, Update), Hist] {
      lazy val UpdateStream = new OutputTag[Update]("UpdateStream")

      override def processElement(value: (Hist, Update), ctx: ProcessFunction[(Hist, Update), Hist]#Context, out: Collector[Hist]): Unit = {
        out.collect(value._1)
        ctx.output(UpdateStream, value._2)
      }

    })

    val updateStream = histStream.getSideOutput(new OutputTag[Update]("UpdateStream"))

    val config:Config = ConfigFactory.load()

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
        val formatter: SimpleDateFormat =  if(data.oldstatusdate.contains("-")) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        else if(data.oldstatusdate.contains("/")) new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
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


    val updateSql = "INSERT INTO APIPRO.EAS_EQUIPMENT_STATUS_UPDATE (NAME,STATUSCODENAME,LASTSTATUSDATE,PRDTIMEFLAG) VALUES (?,?,?,?)"

    updateStream.addSink(new JdbcSink[Update](conf, updateSql, new JdbcWriter[Update] {
      override def accept(stmt: PreparedStatement, data: Update): Unit = {
        val formatter: SimpleDateFormat =  if(data.laststatusdate.contains("-")) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        else if(data.laststatusdate.contains("/")) new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        else new SimpleDateFormat("yyyyMMddHHmmss")
        stmt.setString(1, data.name)
        stmt.setString(2, data.statuscodename)
        stmt.setTimestamp(3, new Timestamp(formatter.parse(data.laststatusdate).getTime))
        stmt.setString(4, "false")
      }
    }))
  }

  case class Hist(name: String, statuscodename: String, newstatusname: String, oldstatusname: String, laststatusdate: String, oldstatusdate: String, usetime: String = null, updatetime: String = null, tubrid: String)

  case class Update(name: String, statuscodename: String, laststatusdate: String, prdtimeflag: String)

}





