package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

class EveConfig extends FLinkKafkaConfig {
  @Option(names = Array("-tm", "--topic.mapping"), required = false)
  var topicMapping: Map[String, String] = new util.LinkedHashMap[String, String]()
}
