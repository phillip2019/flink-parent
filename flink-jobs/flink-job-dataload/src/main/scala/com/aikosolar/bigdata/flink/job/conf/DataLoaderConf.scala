package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaHBaseConfig
import picocli.CommandLine.Option

class DataLoaderConf extends FLinkKafkaHBaseConfig {

  @Option(names = Array("--hbase.table"), required = true, description = Array("Hbase Table Name"))
  var tableName: String = _

  @Option(names = Array("-f", "--field.mapping"), required = false, description = Array("字段映射"))
  var fieldMapping: Map[String, String] = new util.LinkedHashMap[String, String]()
}
