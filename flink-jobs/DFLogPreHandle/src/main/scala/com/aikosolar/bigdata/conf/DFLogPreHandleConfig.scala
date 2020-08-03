package com.aikosolar.app.conf

import java.util

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

class DFLogPreHandleConfig extends FLinkKafkaConfig {

  @Option(names = Array("--target.bootstrap.servers"), required = true)
  var targetBootstrapServers: String = _

  @Option(names = Array("--target.topic"), required = true)
  var targetTopic: String = _

  @Option(names = Array("--target.miss.begin.topic"), required = true)
  var targetMissBeginTopic: String = _

  @Option(names = Array("--target.miss.end.topic"), required = true)
  var targetMissEndTopic: String = _

  override def toMap: util.Map[String, String] = {
    val map: util.Map[String, String] = super.toMap

    map.put("--target.bootstrap.servers", this.targetBootstrapServers)
    map.put("--target.topic", this.targetTopic)
    map.put("--target.miss.begin.topic", this.targetMissBeginTopic)
    map.put("--target.miss.end.topic", this.targetMissEndTopic)

    map
  }


}
