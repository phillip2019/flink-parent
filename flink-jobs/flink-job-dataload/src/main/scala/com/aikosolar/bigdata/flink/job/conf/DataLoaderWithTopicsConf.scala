package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaHBaseConfig
import picocli.CommandLine.Option

class DataLoaderWithTopicsConf extends FLinkKafkaHBaseConfig {

  @Option(names = Array("--hbase.table"), required = false, description = Array("Hbase Table Name"))
  var tableName: String = _

  @Option(names = Array("-tm","--table.mapping"), required = false, description = Array("topic与table映射"))
  var tableMapping: Map[String, String] = new util.LinkedHashMap[String, String]()

  @Option(names = Array("-f", "--field.mapping"), required = false, description = Array("字段映射"))
  var fieldMapping: Map[String, String] = new util.LinkedHashMap[String, String]()

}
