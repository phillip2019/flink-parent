package com.aikosolar.bigdata.flink.job

import java.util
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.utils.Strings
import com.aikosolar.bigdata.flink.job.conf.TubeBoatJobConfig
import com.alibaba.fastjson.JSON
import org.apache.commons.collections.MapUtils
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._


/**
  *
  * --job-name=TubeBoatJobV2
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=carlc4Test
  * --topic=data-collection-se-cycle
  * --reset.strategy=earliest
  * --hbase.table=ods:ods_f_eqp_tube_boat_full
  *
  * @author carlc
  */
object TubeBoatJobV3 extends FLinkKafkaRunner[TubeBoatJobConfig] {

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: TubeBoatJobConfig, rawKafkaSource: DataStream[String]): Unit = {
   rawKafkaSource.map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey.toLowerCase, en.getKey.toLowerCase()), en.getValue)
        }
        result
      })
      .filter(x => Strings.isValidEqpId(x.get("eqpid"))
        && Strings.isValidDataTime(MapUtils.getString(x, "puttime", "")))
      .keyBy(x => (
      x.get("eqp_id").toString,
      x.get("run_count1").toString,
      x.get("run_count2").toString,
      x.get("run_count3").toString,
      x.get("run_count4").toString,
      x.get("run_count5").toString
    )
    ).process(new KeyedProcessFunction[(String,String,String,String,String,String),Map[String, AnyRef],Map[String, AnyRef]]{
      lazy val sendFlg = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("previous",classOf[Boolean]))
      override def processElement(value: util.Map[String, AnyRef], ctx: KeyedProcessFunction[(String, String, String, String, String, String), util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context, out: Collector[util.Map[String, AnyRef]]): Unit = {
        // sendflg == null || false
        // 判断当前是否能
        // todo 设置ttl
      }
    })

  }

}
