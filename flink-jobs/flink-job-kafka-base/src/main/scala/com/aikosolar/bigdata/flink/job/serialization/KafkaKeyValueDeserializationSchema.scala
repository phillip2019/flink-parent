package com.aikosolar.bigdata.flink.job.serialization

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord


/**
  * Kafka消息反序列化器
  *
  * @author carlc
  */
class KafkaKeyValueDeserializationSchema(keyType: String = "topic", charset: String = "utf-8") extends KafkaDeserializationSchema[(String, String)] {

  override def isEndOfStream(t: (String, String)) = false

  override def deserialize(r: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) =
    if ("topic".equals(keyType))
      (r.topic(), new String(r.value(), charset))
    else
      (new String(r.key(), charset), new String(r.value(), charset))


  override def getProducedType: TypeInformation[(String, String)] = Types.TUPLE[(String, String)]
}
