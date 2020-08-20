package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.job.EqpStatus2OracleJob.Update
import com.aikosolar.bigdata.flink.job.conf.DataLoaderConf
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.log4j.Logger
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

object GpDataLoaderJob extends FLinkKafkaRunner[DataLoaderConf] {
  val logger: Logger = Logger.getLogger(GpDataLoaderJob.getClass)

  /**
   * 业务方法[不需自己调用env.execute()]
   */
  override def run0(env: StreamExecutionEnvironment, c: DataLoaderConf, rawKafkaSource: DataStream[String]): Unit = {
    //rawKafkaSource.print("rawKafkaSource")
    val EqStateStream=rawKafkaSource.map(JSON.parseObject(_))
     .process(new ProcessFunction[JSONObject,JSONObject]{

      lazy val MtlEvtDataStream = new OutputTag[JSONObject]("MtlEvtDataStream")

      override def processElement(value: JSONObject, ctx: ProcessFunction[JSONObject, JSONObject]#Context, out: Collector[JSONObject]): Unit = {
        val data=value.getJSONObject("Data")
        if(data!=null && data.size > 0) {
          val data_key=data.keySet()
          if(data_key.contains("EqState")) {
            out.collect(value)
          } else if(data_key.contains("MtlEvtData") ){
            ctx.output(MtlEvtDataStream, value)
          }
        }
      }
    })

    val MtlEvtDataStream = EqStateStream.getSideOutput(new OutputTag[JSONObject]("MtlEvtDataStream"))

    EqStateStream.print("EqStateStream")
    MtlEvtDataStream.print("MtlEvtDataStream")
  }


}
