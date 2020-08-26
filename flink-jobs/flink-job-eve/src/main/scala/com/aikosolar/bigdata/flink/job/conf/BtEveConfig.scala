package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.{FLinkKafkaConfig, FLinkKafkaHBaseConfig}
import picocli.CommandLine.Option

/**
  *
  * @author carlc
 *
 *
 * **/
class BtEveConfig extends FLinkKafkaHBaseConfig {
//  @Option(names = Array("-tm", "--topic.mapping"), required = false, description = Array("主题映射"))
//  var topicMapping: Map[String, String] = new util.LinkedHashMap[String, String]()

  @Option(names = Array("-f", "-fm", "--field.mapping"), required = false, description = Array("字段映射"))
  var fieldMapping: Map[String, String] = new util.LinkedHashMap[String, String]()

  @Option(names = Array("-hbm","--hbase.table.mapping"), required = true, description = Array("Hbase Table Name"))
  var tableNameMapping: Map[String, String] = new util.LinkedHashMap[String, String]()






}

