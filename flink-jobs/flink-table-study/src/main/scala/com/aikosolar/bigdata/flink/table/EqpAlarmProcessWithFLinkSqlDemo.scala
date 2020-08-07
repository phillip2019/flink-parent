package com.aikosolar.bigdata.flink.table

import java.util.{HashMap, Map, Properties}

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

/**
  * 消费kafka(data-collection-eqp-alarm)demo
  * 并过滤出status = 'PRD'的数据
  * 
  * 问题: 
  *   1.为什么既然用了Table Api/SQL不直接用kafka ddl 连接器?
  *     因为:
  *       1. kafka中的数据存在大量的垃圾数据，而ddl方式要求key必须存在，否则报错
  *       2. kafka中key值不规范,所以用DataStream先做了一次转换(情系)
  *       
  *   2. DataStream中为什么要转换为样例类?
  *     因为:
  *       1. 后续处理基于FLink SQL,HashMap不知道为什么FLink读不出来schema,所以后续写SQL 字段找不到,所以.....
  * @author carlc
  */
object EqpAlarmProcessWithFLinkSqlDemo {

  def getValue(result: Map[String, AnyRef], key: String): String = {
    val v = result.get(key)
    if (v == null) null else v.toString
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "172.16.111.20:9092,172.16.111.21:9092,172.16.111.22:9092")
    props.setProperty("group.id", "carlc4Test")
    props.setProperty("enable.auto.commit", "true")
    val source: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String]("data-collection-eqp-alarm", new SimpleStringSchema, props)

    source.setStartFromEarliest()

    val kafkaSource: DataStream[EqpAlarm] = env.addSource(source)
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(en.getKey.toLowerCase(), en.getValue)
        }

        val eqpid = getValue(result, "eqpid")
        val puttime = getValue(result, "puttime")
        val status = getValue(result, "status")
        val alarmcode = getValue(result, "alarmcode")
        val alarmtext = getValue(result, "alarmtext")
        val tubeid1 = getValue(result, "tubeid1")
        val tubeid2 = getValue(result, "tubeid2")
        val tubeid3 = getValue(result, "tubeid3")
        val tubeid4 = getValue(result, "tubeid4")
        val tubeid5 = getValue(result, "tubeid5")
        EqpAlarm(eqpid, puttime, status, alarmcode, alarmtext, tubeid1, tubeid2, tubeid3, tubeid4, tubeid5)
      })
      .filter(x => Strings.isValidEqpId(x.eqpid))

    val kafkaTable: Table = tEnv.fromDataStream(kafkaSource)
    // 注册视图,后续计算用
    tEnv.registerTable("eqp_alarm", kafkaTable)
    val query =
      """
        |SELECT eqpid,puttime,status,tubeid1,tubeid2,tubeid3,tubeid4,tubeid5
        |FROM eqp_alarm
        |WHERE status = 'PRD'
      """.stripMargin

    val result = tEnv.sqlQuery(query)
    result.toAppendStream[Row].print()

    // ... 写入HBase(官方直接支持)/jdbc(官方直接支持)

    env.execute("FLinkSqlDemo")
  }
}

case class EqpAlarm(eqpid: String, puttime: String, status: String,
                    alarmcode: String, alarmtext: String, tubeid1: String,
                    tubeid2: String, tubeid3: String, tubeid4: String, tubeid5: String)