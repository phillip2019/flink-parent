package com.aikosolar.bigdata.conf

import java.util

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

/**
  * @author carlc
  */
class HalmAlarmConfig extends FLinkKafkaConfig {

  //------------------------输出kafka信息--------------------------------
  @Option(names = Array("--target.bootstrap.servers"), required = true)
  var targetBootstrapServers: String = _

  @Option(names = Array("--target.topic"), required = true)
  var targetTopic: String = _

  //------------------------窗口大小-------------------------------------
  @Option(names = Array("--windows.seconds.size"), required = true)
  var windowsSize: Int = 5 * 60

  //------------------------mysql连接信息--------------------------------
  @Option(names = Array("--mysql.url"), required = true)
  var mysqlUrl: String = _
  @Option(names = Array("--mysql.username"), required = true)
  var mysqlUsername: String = _
  @Option(names = Array("--mysql.password"), required = true)
  var mysqlPassword: String = _
  @Option(names = Array("--mysql.sql"), required = true)
  var mysqlSQL: String = _

  override def toMap: util.Map[String, String] = {
    val map: util.Map[String, String] = super.toMap

    map.put("--target.bootstrap.servers", this.targetBootstrapServers)
    map.put("--target.topic", this.targetTopic)

    map.put("--windows.seconds.size", this.windowsSize.toString)

    map.put("--mysql.url", this.mysqlUrl)
    map.put("--mysql.username", this.mysqlUsername)
    map.put("--mysql.password", this.mysqlPassword)
    map.put("--mysql.sql", this.mysqlSQL)

    map
  }


}