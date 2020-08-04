package com.aikosolar.bigdata.flink.job

import java.util.Properties
import java.util.function.BiConsumer
import java.util.regex.Pattern

import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._

/**
  * 流计算通常都是基于Kafka(输入)
  * 因流计算基于Kafka作为输入太常用了,所以统一写一个吧,减少代码量以及bug
  *
  * @author carlc
  */
abstract class FLinkKafkaRunner[C <: FLinkKafkaConfig] extends FLinkRunner[C] {

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
    val source: FlinkKafkaConsumer010[String] = if (c.topicRegex) {
      new FlinkKafkaConsumer010[String](Pattern.compile(c.topic), new SimpleStringSchema, props)
    } else {
      new FlinkKafkaConsumer010[String](c.topic.split(",").toList, new SimpleStringSchema, props)
    }
    c.resetStrategy.toLowerCase() match {
      case "earliest" => source.setStartFromEarliest()
      case "latest" => source.setStartFromLatest()
      case "groupoffsets" => source.setStartFromGroupOffsets()
      case "none" =>
    }
    val rawKafkaSource: DataStream[String] = env.addSource(source)
    run0(env, c, rawKafkaSource)
  }

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  def run0(env: StreamExecutionEnvironment, c: C, rawKafkaSource: DataStream[String]): Unit
}