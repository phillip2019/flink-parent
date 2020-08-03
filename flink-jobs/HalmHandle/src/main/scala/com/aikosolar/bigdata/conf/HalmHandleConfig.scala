package com.aikosolar.app.conf

import java.util

import com.aikosolar.bigdata.flink.job.base.config.FLinkKafkaConfig
import picocli.CommandLine.Option

class HalmHandleConfig extends FLinkKafkaConfig {

  @Option(names = Array("--hbase.table"), required = true, description = Array("Hbase Table Name"))
  var tablename: String = _

  @Option(names = Array("--hbase.writeBufferSize"), required = false, description = Array("Hbase WriteBufferSize"))
  var writeBufferSize: String = (10 * 1024 * 1024).toString

  @Option(names = Array("--hbase.count"), required = false, description = Array("Hbase count"))
  var count: String = 10000.toString

  @Option(names = Array("--hbase.durability"), required = false, description = Array("Hbase Durability"))
  var setDurability: String = _

  @Option(names = Array("--hbase.async"), required = false, description = Array("Hbase async"))
  var async: Boolean = false


  override def toMap: util.Map[String, String] = {
    val map: util.Map[String, String] = super.toMap

    map.put("--bootstrap.servers", this.tablename)
    map.put("--group.id", this.writeBufferSize)
    map.put("--topic", this.count)
    map
  }


}
