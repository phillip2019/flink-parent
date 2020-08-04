package com.aikosolar.bigdata.flink.job

import java.util.Properties
import java.util.regex.Pattern

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._

/**
  * 流计算通常都是基于Kafka(输入)
  *
  * @author carlc
  */
abstract class FLinkKafkaRunner[C <: FLinkKafkaConfig] extends FLinkKafkaBaseRunner[C, String] {
  /**
    * 获取FlinkKafkaConsumer010
    */
  override protected def getKafkaConsumer(c: C, props: Properties): FlinkKafkaConsumer010[String] = {
    if (c.topicRegex)
      new FlinkKafkaConsumer010[String](Pattern.compile(c.topic), new SimpleStringSchema, props)
    else
      new FlinkKafkaConsumer010[String](c.topic.split(",").toList, new SimpleStringSchema, props)
  }
}
