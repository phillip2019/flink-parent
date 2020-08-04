package com.aikosolar.bigdata.flink.job

import java.util.Properties
import java.util.regex.Pattern

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import com.aikosolar.bigdata.flink.job.serialization.KafkaTopicDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._

/**
  * kafka消息被反序列化为(String /* topic */,String /* message */)
  *
  * @author carlc
  */
abstract class FLinkKafkaWithTopicRunner[C <: FLinkKafkaConfig] extends FLinkKafkaBaseRunner[C, (String /*topic*/ , String /*message*/ )] {
  /**
    * 获取FlinkKafkaConsumer010
    */
  override protected def getKafkaConsumer(c: C, props: Properties): FlinkKafkaConsumer010[(String, String)] = {
    if (c.topicRegex)
      new FlinkKafkaConsumer010[(String, String)](Pattern.compile(c.topic), new KafkaTopicDeserializationSchema, props)
    else
      new FlinkKafkaConsumer010[(String, String)](c.topic.split(",").toList, new KafkaTopicDeserializationSchema, props)
  }
}
