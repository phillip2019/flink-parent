package com.aikosolar.bigdata.flink.job

import java.util.Properties
import java.util.function.BiConsumer
import java.util.regex.Pattern

import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import com.aikosolar.bigdata.flink.job.serialization.KafkaKeyValueDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._

/**
  * kafka消息被反序列化为(String /* topic */,String /* message */)
  * @author carlc
  */
abstract class FLinkKafkaWithTopicRunner[C <: FLinkKafkaConfig] extends FLinkRunner[C] {
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
    val deserialization = new KafkaKeyValueDeserializationSchema(keyType = getKeyType(), charset = getCharset())
    val source: FlinkKafkaConsumer010[(String, String)] = if (c.topicRegex) {
      new FlinkKafkaConsumer010[(String, String)](Pattern.compile(c.topic), deserialization, props)
    } else {
      new FlinkKafkaConsumer010[(String, String)](c.topic.split(",").toList, deserialization, props)
    }
    c.resetStrategy.toLowerCase() match {
      case "earliest" => source.setStartFromEarliest()
      case "latest" => source.setStartFromLatest()
      case "groupoffsets" => source.setStartFromGroupOffsets()
      case "none" =>
    }
    val rawKafkaSource: DataStream[(String, String)] = env.addSource(source)
    run0(env, c, rawKafkaSource)
  }

  def getKeyType(): String = {
    "topic"
  }

  def getCharset(): String = {
    "utf-8"
  }

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  def run0(env: StreamExecutionEnvironment, c: C, rawKafkaSource: DataStream[(String, String)]): Unit
}
