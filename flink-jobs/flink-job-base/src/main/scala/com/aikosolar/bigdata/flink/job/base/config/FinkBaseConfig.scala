package com.aikosolar.bigdata.flink.job.base.config

import java.util

import org.apache.flink.api.common.ExecutionConfig
import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class FinkBaseConfig extends ExecutionConfig.GlobalJobParameters {

  @Option(names = Array("--job-name"), required = true)
  var jobName: String = _

  @Option(names = Array("--time-characteristic"), required = false)
  var timeCharacteristic: String = "ProcessingTime"

  @Option(names = Array("--runMode"), required = false, description = Array("运行模式:prod|test"))
  var runMode: String = "test"

  @Option(names = Array("--checkpointDataUri"), required = false, description = Array("当运行模式为prod必传"))
  var checkpointDataUri: String = _

  @Option(names = Array("--cpInterval"), required = false)
  var cpInterval: Long = 5000L

  @Option(names = Array("--cpMode"), required = false)
  var cpMode: String = "EXACTLY_ONCE" // AT_LEAST_ONCE

  @Option(names = Array("--cpMinPauseBetweenCheckpoints"), required = false)
  var cpMinPauseBetweenCheckpoints: Long = 1000L

  @Option(names = Array("--cpTimeout"), required = false)
  var cpTimeout: Long = 60000L

  @Option(names = Array("--cpMaxConcurrentCheckpoints"), required = false)
  var cpMaxConcurrentCheckpoints: Int = 1

  @Option(names = Array("--cpExternalizedCheckpointCleanup"), required = false)
  var cpExternalizedCheckpointCleanup: String = "RETAIN_ON_CANCELLATION"

  override def toMap: util.Map[String, String] = {
    val map: util.Map[String, String] = new util.HashMap[String, String]

    map.put("--job-name", this.jobName)
    map.put("--time-characteristic", this.timeCharacteristic)
    map.put("--runMode", this.runMode)
    map.put("--checkpointDataUri", this.checkpointDataUri)

    map
  }
}