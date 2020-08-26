package com.aikosolar.bigdata.flink.job.base.config

import java.util
import java.util.Map

import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class FLinkKafkaHBaseConfig extends FLinkKafkaConfig {

  @Option(names = Array("--hbase.config"), required = false, description = Array("HBase配置"))
  var hbaseConfig: Map[String, String] = new util.LinkedHashMap[String, String]()

  override def toMap: util.Map[String, String] = {

    val map: util.Map[String, String] = super.toMap

    map.put("--bootstrap.servers", this.bootstrapServers)
    map.put("--group.id", this.groupId)
    map.put("--topic", this.topic)

    map
  }
}
