package com.aikosolar.bigdata.flink.job.conf

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

class AllEqpConfig extends FLinkKafkaConfig {
  @Option(names = Array("-f", "--field.mapping"), required = false, description = Array("字段映射"))
  var fieldMapping: Map[String, String] = new util.LinkedHashMap[String, String]()

//  rowkey就不用了
//  @Option(names = Array("-r", "--rowkey.fields"), required = false, description = Array("RowKey字段列表(转换后的字段名称)"))
//  var rowKeyFields: util.List[String] = util.ArrayList[String]
}
