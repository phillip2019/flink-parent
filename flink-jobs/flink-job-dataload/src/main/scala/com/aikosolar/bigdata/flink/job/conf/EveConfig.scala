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

  @Option(names = Array("--hbase.table"), required = false, description = Array("Hbase Table Name"))
  var tableName: String = _

  @Option(names = Array("--mysql.url"), required = false)
  var mysqlUrl: String = _
  @Option(names = Array("--mysql.username"), required = false)
  var mysqlUsername: String = _
  @Option(names = Array("--mysql.password"), required = false)
  var mysqlPassword: String = _
  @Option(names = Array("--mysql.sql"), required = false)
  var mysqlSQL: String = _
}
