package com.aikosolar.bigdata.flink.job.conf

import com.aikosolar.bigdata.flink.job.base.config.{FLinkKafkaConfig, FLinkKafkaHBaseConfig}
import picocli.CommandLine.Option

/**
  * @author carlc
  */
class GpDataLoaderConf extends FLinkKafkaHBaseConfig {

  @Option(names = Array("--gp1.hbase.table"), required = true, description = Array("Hbase Table Name"))
  var gp1tableName: String = _

  @Option(names = Array("--target.bootstrap.servers"), required = true)
  var targetBootstrapServers: String = _

  @Option(names = Array("--target.topic"), required = true)
  var targetTopic: String = _

  @Option(names = Array("--gp2.hbase.table"), required = true, description = Array("Hbase Table Name"))
  var gp2tableName: String = _

}
