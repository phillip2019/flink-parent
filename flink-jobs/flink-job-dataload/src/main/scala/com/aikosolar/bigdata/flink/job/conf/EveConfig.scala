package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class EveConfig extends FLinkKafkaConfig {
  @Option(names = Array("-tm", "--topic.mapping"), required = false)
  var topicMapping: Map[String, String] = new util.LinkedHashMap[String, String]()


  @Option(names = Array("--mysql.url"), required = true)
  var mysqlUrl: String = _
  @Option(names = Array("--mysql.username"), required = true)
  var mysqlUsername: String = _
  @Option(names = Array("--mysql.password"), required = true)
  var mysqlPassword: String = _
  @Option(names = Array("--mysql.sql"), required = true)
  var mysqlSQL: String = _
}
