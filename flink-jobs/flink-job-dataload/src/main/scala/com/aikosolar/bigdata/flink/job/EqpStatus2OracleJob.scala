package com.aikosolar.bigdata.flink.job

import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.text.SimpleDateFormat
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.aikosolar.bigdata.flink.connectors.jdbc.{JdbcMergeSink, JdbcSink}
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions
import com.aikosolar.bigdata.flink.connectors.jdbc.writter.JdbcWriter
import com.aikosolar.bigdata.flink.job.conf.AllEqpConfig
import com.alibaba.fastjson.JSON
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._


/**
  *
  * 所有设备Status日志数据采集
  *
  * input: kafka  topic: data-collection-eqp-status
  * kafka 消息格式：
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
  */
object EqpStatus2OracleJob extends FLinkKafkaRunner[AllEqpConfig] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: AllEqpConfig, rawKafkaSource: DataStream[String]): Unit = {
    val histStream: DataStream[Hist] = rawKafkaSource
      .map(line=>{
        JSON.parseObject(line)
      })
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
      .filter(r=>r.containsKey("newstatus"))
      .filter(r=>r.containsKey("oldstatus"))
      .filter(r=>r.containsKey("newtime"))
      .filter(r=>r.containsKey("oldtime"))
      .filter(r => Strings.isValidEqpId(r.get("eqpid")))
      .map(m => {
        val eqpid = m.get("eqpid").toString
        val newstatus = MapUtils.getString(m, "newstatus", "")
        val oldstatus =MapUtils.getString(m, "oldstatus", "")
        val newtime = MapUtils.getString(m, "newtime", "")
        val oldtime =MapUtils.getString(m, "oldtime", "")
        val tubeid = MapUtils.getString(m, "tubeid", "")

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

    val histSql =
      """
        |INSERT INTO APIPRO.EAS_EQUIPMENT_STATUS_HIST
        | (NAME, STATUSCODENAME, NEWSTATUSNAME, OLDSTATUSNAME, LASTSTATUSDATE, OLDSTATUSDATE, USETIME, UPDATETIME, TUBEID)
        | VALUES(?,?,?,?,?,?,?,?,?)
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
        stmt.setTimestamp(5, new Timestamp(formatter.parse(data.laststatusdate).getTime))
        stmt.setTimestamp(6, new Timestamp(formatter.parse(data.oldstatusdate).getTime))
        stmt.setObject(7, null) //todo 确认
        stmt.setObject(8, null)
        stmt.setString(9,data.tubeid)
      }
    }))



    val updateSql="update APIPRO.EAS_EQUIPMENT_STATUS_UPDATE_K set STATUSCODENAME=?,LASTSTATUSDATE=?,PRDTIMEFLAG=? where NAME=?"
    val insertSql = "INSERT INTO APIPRO.EAS_EQUIPMENT_STATUS_UPDATE_K (NAME,STATUSCODENAME,LASTSTATUSDATE,PRDTIMEFLAG) VALUES (?,?,?,?)"

    updateStream.addSink(new JdbcMergeSink[Update](conf,  updateSql, insertSql, new JdbcWriter[Update] {
      override def  update(stmt: PreparedStatement, data: Update){
        val formatter: SimpleDateFormat =  if(data.laststatusdate.contains("-")) new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        else if(data.laststatusdate.contains("/")) new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        else new SimpleDateFormat("yyyyMMddHHmmss")
        stmt.setString(4, data.name)
        stmt.setString(1, data.statuscodename)
        stmt.setTimestamp(2, new Timestamp(formatter.parse(data.laststatusdate).getTime))
        stmt.setString(3, "false")

      }

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


    if(!"prod".equals(c.runMode)){
      histStream.print("histStream")
      updateStream.print("updateStream")
    }

  }

  case class Hist(name: String, statuscodename: String, newstatusname: String, oldstatusname: String, laststatusdate: String, oldstatusdate: String, usetime: String = null, updatetime: String = null, tubeid: String)

  case class Update(name: String, statuscodename: String, laststatusdate: String, prdtimeflag: String)

}





