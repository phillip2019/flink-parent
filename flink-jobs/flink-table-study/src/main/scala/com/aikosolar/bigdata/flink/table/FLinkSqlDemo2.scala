//package com.aikosolar.bigdata.flink.table
//
//import java.util.{HashMap, Map, Properties}
//
//import com.aikosolar.bigdata.flink.common.utils.Strings
//import com.alibaba.fastjson.JSON
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
//import org.apache.flink.table.api.scala._
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//import org.apache.flink.types.Row
//
//import scala.collection.JavaConversions._
//
///**
//  * @author carlc
//  */
//object FLinkSqlDemo2 {
//
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val settings = EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inStreamingMode()
//      .build()
//
//    val tEnv = StreamTableEnvironment.create(env, settings)
//
//    val props = new Properties()
//    props.setProperty("bootstrap.servers", "172.16.111.20:9092,172.16.111.21:9092,172.16.111.22:9092")
//    props.setProperty("group.id", "carlc4Test")
//    props.setProperty("enable.auto.commit", "true")
//    val source: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String]("data-collection-eqp-alarm", new SimpleStringSchema, props)
//
//    source.setStartFromEarliest()
//
//    val kafkaSource: DataStream[AlarmBean] = env.addSource(source)
//      .map(JSON.parseObject(_))
//      .map(jsonObj => {
//        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
//        for (en <- jsonObj.entrySet) {
//          result.putIfAbsent(en.getKey.toLowerCase(), en.getValue)
//        }
//        result
//      })
//      .filter(x => Strings.isValidEqpId(x.get("eqpid")))
//      .map(Utils.toJson(_)) // todo 这一步不友好
//    // DataStream - > Table
//    val kafkaTable: Table = tEnv.fromDataStream(kafkaSource)
//    // 注册视图,后续计算用
//    tEnv.registerTable("eqp_alarm", kafkaTable) // todo 这一步API会和后续版本不一致
//    val query =
//      """
//        |SELECT eqpid,puttime,status,tubeid1,tubeid2,tubeid3,tubeid4,tubeid5   -- 简化字段
////        |SELECT eqpid,puttime,status,alarmcode,alarmtext,tubeid1,tubeid2,tubeid3,tubeid4,tubeid5   -- 简化字段
////        |SELECT eqpid,status   -- 简化字段
//        |FROM eqp_alarm
////        |WHERE status is 'PRD'
//        |WHERE status = 'PRD'
//      """.stripMargin
//
//    val result = tEnv.sqlQuery(query)
//    result.toAppendStream[Row].print()
//    env.execute("FLinkSqlDemo")
//  }
//}