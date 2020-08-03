package com.aikosolar.bigdata

import java.util.UUID

import com.aikosolar.bigdata.bean.{EqpHalm, EqpHalmWithStatus, HalmAlarm}
import com.aikosolar.bigdata.conf.HalmAlarmConfig
import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.Dates
import com.aikosolar.bigdata.flink.job.FLinkKafkaRunner
import com.aikosolar.bigdata.source.MySQLEqpHalmSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.util.Collector
import org.json4s.native.Serialization

/**
  *
  * Halm告警任务
  *
  * <ul>需求:
  * <li>1. 从特定Kafka Topic消费数据(Halm类型:Halm01,Halm02,Halm03,...)</li>
  * <li>2. 如果某个Halm在指定时间范围来了数据，则进行统计计算,并写入Kakfa</li>
  * <li>3. 如果某个Halm在特定时间范围内没有过来数据,仍然需要发送统计数据(0)</li>
  * </ul>
  *
  * 输入topic:  data-collection-halm-yw
  * 输出topic:  eqp_ct_state_alarm
  *
  * @author carlc
  */
object HalmAlarmJob extends FLinkKafkaRunner[HalmAlarmConfig] {

  implicit val formats = org.json4s.DefaultFormats

  //--job-name=HalmAlarmJob
  //--time-characteristic=ProcessingTime
  //--bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  //--group.id=carlc4Test
  //--topic=data-collection-halm-yw
  //--target.bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  //--target.topic=eqp_ct_state_alarm
  //--windows.seconds.size=5
  //--mysql.url=jdbc:mysql://172.16.98.89:3306/eqp?useSSL=false&useUnicode=true&characterEncoding=UTF-8
  //--mysql.username=root
  //--mysql.password=123456
  //--mysql.sql="select * from eqp.halm where state='PRD'"

  val STATUS_ERR = 0
  val STATUS_OK = 1

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: HalmAlarmConfig, rawKafkaSource: DataStream[String]): Unit = {
    // 真实数据
    val kafkaStream = rawKafkaSource
      .map(json => EqpHalm(json))
      .filter(_ != null)

    // 模拟流(推动流)
    val pushStream = env.addSource(new MySQLEqpHalmSource(
      (if (c.windowsSize > 60) c.windowsSize / 2 - 1 else 1) * 1000L)
    )

    // 告警流(结果流)
    val alertStream = pushStream.connect(kafkaStream)
      .map(x1 => {
        EqpHalmWithStatus(x1.eqpId, STATUS_ERR, x1.ts)
      }, x2 => {
        EqpHalmWithStatus(x2.eqpId, STATUS_OK, x2.ts)
      })
      .keyBy(_.eqpId)
      .timeWindow(Time.seconds(c.windowsSize))
      .aggregate(new PreAggregator, new WindowFunc)
      .map(x => {
        val json: String = Serialization.write(x)
        json
      })

    alertStream.addSink(new FlinkKafkaProducer010[String](c.targetBootstrapServers, c.targetTopic, new SimpleStringSchema()))
  }

  class PreAggregator extends AggregateFunction[EqpHalmWithStatus, (EqpHalmWithStatus, Int), (EqpHalmWithStatus, Int)] {
    override def createAccumulator(): (EqpHalmWithStatus, Int) = (null, 0)

    override def add(value: EqpHalmWithStatus, acc: (EqpHalmWithStatus, Int)): (EqpHalmWithStatus, Int) = {
      // 站在第1次和第3次的角度去考虑问题
      // 当为第3次时，需要参考第2次的情况
      // 第1次(true|false))，第2次(true|false)，第3次(true|false)
      if (acc._1 == null) {
        (value, if (value.status == STATUS_ERR) 0 else 1)
      } else {
        if (value.status == STATUS_OK) (value, acc._2 + 1) else acc
      }
    }

    override def getResult(acc: (EqpHalmWithStatus, Int)): (EqpHalmWithStatus, Int) = acc

    override def merge(acc1: (EqpHalmWithStatus, Int), acc2: (EqpHalmWithStatus, Int)): (EqpHalmWithStatus, Int) = {
      if (acc1._1 != null) {
        (acc1._1, acc2._2 + acc2._2)
      } else {
        (acc2._1, acc2._2 + acc2._2)
      }
    }
  }

  class WindowFunc extends WindowFunction[(EqpHalmWithStatus, Int), HalmAlarm, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[(EqpHalmWithStatus, Int)], out: Collector[HalmAlarm]): Unit = {
      for (e <- input) {
        val eqpId = e._1.eqpId
        val site = eqpId.substring(0, 2)
        val sub_process = eqpId
        val factory = Sites.toFactoryId(site)
        val beginTime = Dates.long2String(window.getStart, Dates.fmt2)
        val endTime = Dates.long2String(window.getEnd, Dates.fmt2)
        val status = e._1.status
        val cnt = e._2
        val message = if (status == STATUS_OK) null else s"No message received for a long time($eqpId)"
        val alarmId = "%s%s%02d%s".format(Dates.now(Dates.fmt6), eqpId.replaceAll("-", ""), if (status == STATUS_OK) 1 else 0, UUID.randomUUID().toString.replaceAll("-", "").toUpperCase().substring(0, 4))
        val createTime = Dates.now(Dates.fmt2)

        out.collect(HalmAlarm(alarmId, factory, site, eqpId, sub_process, beginTime, endTime, status, cnt, message, createTime))
      }
    }
  }

}

