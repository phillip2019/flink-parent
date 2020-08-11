package com.aikosolar.bigdata.flink.job

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.aikosolar.bigdata.flink.job.conf.EveConfig
import com.aikosolar.bigdata.flink.job.enums.EveStep
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * todo 1. 重命名
  *
  * @author carlc
  */
object EveJob extends FLinkKafkaWithTopicRunner[EveConfig] {


  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: EveConfig, rawKafkaSource: DataStream[(String, String)]): Unit = {
    rawKafkaSource
      .map(x => (c.topicMapping.getOrDefault(x._1, null), JSON.parseObject(x._2, classOf[Subscription]))) // topic转换
      .filter(_._1 != null)
      .filter(x => StringUtils.isNotBlank(x._2.tubeId) && Strings.isValidEqpId(x._2.eqpId))
      .map(x => {
        val data: Subscription = x._2
        val tagService = EveTagServiceFactory.getEveTagService(x._1)
        if (tagService != null) {
          data.tag = tagService.tag(data.text1)
        }
        data
      })
      .filter(_.tag != null)
      .keyBy(x => (x.eqpId, x.tubeId))
      .process(new EveFunction())
  }

  case class Subscription(eqpId: String, tubeId: String, states: String,
                          text1: String, putTime: String, runTime: String,
                          runCount: String, var tag: EveStep = null) // todo 还要添加其他字段


  class EveFunction extends KeyedProcessFunction[(String, String), Subscription, Subscription] {
    lazy val previous = getRuntimeContext.getState(new ValueStateDescriptor[Subscription]("previous", classOf[Subscription]))

    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
      //todo
    }
  }

}
