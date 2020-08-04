package com.aikosolar.bigdata.flink.job

import java.util.Properties
import java.util.function.BiConsumer

import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @author carlc
  */
abstract class FLinkKafkaBaseRunner[C <: FLinkKafkaConfig, E] extends FLinkRunner[C] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run(env: StreamExecutionEnvironment, c: C): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", c.asInstanceOf[FLinkKafkaConfig].bootstrapServers)
    props.setProperty("group.id", c.asInstanceOf[FLinkKafkaConfig].groupId)
    // 设置kafka其他参数
    c.asInstanceOf[FLinkKafkaConfig].kafkaConf.forEach(new BiConsumer[String, String] {
      override def accept(k: String, v: String): Unit = {
        props.put(k, v)
      }
    })
    // 当开启checkpoint,[enable.auto.commit]和[auto.commit.interval.ms]参数将被忽略
    // 这里设置的目的仅仅为了在KafkaManager中能看到消费组偏移量
    props.setProperty("enable.auto.commit", "true")
    val source: FlinkKafkaConsumer010[E] = getKafkaConsumer(c, props)

    val rawKafkaSource: DataStream[E] = env.addSource(source)
    run0(env, c, rawKafkaSource)
  }

  /**
    * 获取FlinkKafkaConsumer010
    */
  protected def getKafkaConsumer(c: C, props: Properties): FlinkKafkaConsumer010[E]

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  def run0(env: StreamExecutionEnvironment, c: C, rawKafkaSource: DataStream[E]): Unit
}
