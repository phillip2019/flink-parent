package com.aikosolar.bigdata.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


/**
  * FLink Table/SQL API 学习
  *
  * 消费kafka消息(eqp_ct_state_alarm)并使用sql过滤出msg为null的数据(正常数据)
  *
  * @author carlc
  */
object EqpStateAlarmProcessBySqlDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    val createTable =
      """
        |CREATE TABLE eqp_ct_state_alarm (
        |    alarm_id VARCHAR,
        |    factory VARCHAR,
        |    site VARCHAR,
        |    eqp_id VARCHAR,
        |    sub_process VARCHAR,
        |    begin_time VARCHAR,
        |    end_time VARCHAR,
        |    status VARCHAR,
        |    cnt VARCHAR,
        |    msg VARCHAR,
        |    create_time VARCHAR
        |)
        |WITH (
        |    'connector.type' = 'kafka',
        |    'connector.version' = '0.10',
        |    'connector.topic' = 'eqp_ct_state_alarm',
        |    'connector.startup-mode' = 'latest-offset',
        |    'connector.properties.0.key' = 'zookeeper.connect',
        |    'connector.properties.0.value' = '172.16.111.20:2181,172.16.111.21:2181,172.16.111.22:2181',
        |    'connector.properties.1.key' = 'bootstrap.servers',
        |    'connector.properties.1.value' = '172.16.111.20:9092,172.16.111.21:9092,172.16.111.22:9092',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',
        |    'format.derive-schema' = 'true'
        |)
      """.stripMargin

    tEnv.sqlUpdate(createTable)

    val query =
      """
      |SELECT eqp_id,begin_time,end_time,cast(cnt AS INT)   -- 简化字段
        |FROM eqp_ct_state_alarm
        |WHERE cast(status AS INT) = 0 -- 因为原始程序把cnt类型设置为了string，后面修正了，所以这里统一转换为int再判断
      """.stripMargin

    val result = tEnv.sqlQuery(query)

    result.toRetractStream[Row].print()

    env.execute("FLinkSqlDemo")
  }
}
