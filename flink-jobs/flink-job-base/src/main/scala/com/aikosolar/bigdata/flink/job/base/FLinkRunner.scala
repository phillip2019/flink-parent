package com.aikosolar.bigdata.flink.job.base

import java.lang.reflect.{ParameterizedType, Type}

import com.aikosolar.bigdata.flink.job.base.config.FinkBaseConfig
import org.apache.commons.lang3.StringUtils
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import picocli.CommandLine

/**
  *
  * @author carlc
  */
abstract class FLinkRunner[C <: FinkBaseConfig] {

  final def main(args: Array[String]): Unit = {
    runWith(args)
  }

  def runWith(args: Array[String]): Unit = {
    for (arg <- args) println(arg)

    val config: C = parseConfig(args)

    this.validate(config)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    this.setupEnv(env, config)

    env.getConfig.setGlobalJobParameters(config)

    this.run(env, config)

    val jName = jobName(config)

    if (jName == null || "".equals(jName.trim)) env.execute() else env.execute(jName)
  }

  def parseConfig(args: Array[String]): C = {
    // 个人不喜欢FLink提供的解析参数工具,所以用第三方的
    val pType: ParameterizedType = (this.getClass.getGenericSuperclass).asInstanceOf[ParameterizedType]
    val actualTypeArguments: Array[Type] = pType.getActualTypeArguments
    val t: Type = actualTypeArguments(0)
    val c: Class[C] = t.asInstanceOf[Class[C]]
    val instance: C = c.newInstance()
    new CommandLine(instance).parse(args: _*)
    instance
  }

  /**
    * 获取任务名称,默认为配置项中jobName
    */
  def jobName(c: C): String = c.jobName

  /**
    * 配置校验
    */

  def validate(c: C): Unit = {
    if ("prod".equalsIgnoreCase(c.runMode)) {
      if (StringUtils.isBlank(c.checkpointDataUri)) {
        throw new IllegalArgumentException("-- checkpointDataUri is required")
      }
    }
  }

  /**
    * 构建/初始化 env
    */
  def setupEnv(env: StreamExecutionEnvironment, c: C): Unit = {
    // 设置时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(c.timeCharacteristic))
    // 开启checkpoint,间隔时间为5s
    env.enableCheckpointing(c.cpInterval)
    // 设置处理模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.valueOf(c.cpMode))
    // 设置两次checkpoint的间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(c.cpMinPauseBetweenCheckpoints)
    // 设置超时时长
    env.getCheckpointConfig.setCheckpointTimeout(c.cpTimeout)
    // 设置并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(c.cpMaxConcurrentCheckpoints)
    // 当程序关闭的时候,触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.valueOf(c.cpExternalizedCheckpointCleanup))
    // 设置检查点在hdfs中存储的位置
    if ("prod".equalsIgnoreCase(c.runMode)) {
      env.setStateBackend(new FsStateBackend(c.checkpointDataUri))
    }
  }

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  def run(env: StreamExecutionEnvironment, c: C)
}
