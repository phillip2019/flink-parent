package com.aikosolar.bigdata.flink.job.base.config

import java.util
import java.util.Map

import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class FLinkKafkaConfig extends FinkBaseConfig {

  @Option(names = Array("--bootstrap.servers"), required = true, description = Array("Kafka Bootstrap Servers"))
  var bootstrapServers: String = _

  @Option(names = Array("--group.id"), required = true, description = Array("Group Id"))
  var groupId: String = _

  @Option(names = Array("--topic"), required = true, description = Array("Kafka Topic"))
  var topic: String = _

  @Option(names = Array("--is.topic.regex"), required = false, description = Array("topic是否为正则形式"))
  var topicRegex: Boolean = false

  @Option(names = Array("--reset.strategy"), required = false, description = Array("kafka消费重置策略(不区分大小写):none(默认)|earliest|latest|GroupOffsets"))
  var resetStrategy: String = "NONE"

  @Option(names = Array("-kc", "--kafka.config"), required = false, description = Array("kafka其他参数"))
  var kafkaConf: Map[String, String] = new util.LinkedHashMap[String, String]()


  override def toMap: util.Map[String, String] = {

    val map: util.Map[String, String] = super.toMap

    map.put("--bootstrap.servers", this.bootstrapServers)
    map.put("--group.id", this.groupId)
    map.put("--topic", this.topic)

    map
  }
}
