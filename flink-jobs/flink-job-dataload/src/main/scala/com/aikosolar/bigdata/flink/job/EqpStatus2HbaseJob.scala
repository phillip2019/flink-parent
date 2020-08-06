package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.SimpleHBaseTableSink
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.conf.DataLoaderConf
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.flink.streaming.api.scala._
import org.apache.log4j.Logger

import scala.collection.JavaConversions._


/**
  *
  * 所有设备Status日志数据采集
  *
  * input: kafka  topic: data-collection-eqp-status
  * kafka 消息格式：
  * "EqpID": "Z2-TAH08",		设备编号
  * "NewStatus": "SBY",		新状态
  * "NewTime": "2020-07-30 08:51:58",	新时间
  * "OldStatus": "PRD",		老状态
  * "OldTime": "2020-07-30 08:51:58"	老状态时间
  *
  * output: oracle
  *
  * 逻辑: 根据业务逻辑将数据数据处理以后写入hbase中,
  *
  * 运行参数:
  *
  * flink run -m yarn-cluster \
  * -p 3 \
  * -ys 2 \
  * -yjm 1024 \
  * -ytm 2048 \
  * -ynm HalmFull \
  * --class com.aikosolar.bigdata.HalmFullJob  /root/halm/HalmHandle-1.1.0.jar \
 * --job-name=DFAlarmJob
 * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
 * --group.id=df-status-group
 * --reset.strategy=latest
 * --hbase.table=xxxx
 * --topic=data-collection-eqp-status
 *
  * @author carlc
  */
object EqpStatus2HbaseJob extends FLinkKafkaRunner[DataLoaderConf] {
  val logger:Logger= Logger.getLogger(EqpStatus2HbaseJob.getClass)
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: DataLoaderConf, rawKafkaSource: DataStream[String]): Unit = {
    val kafkaSource: DataStream[Map[String, AnyRef]] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        var result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          val key = c.fieldMapping.getOrDefault(en.getKey, en.getKey)
          val value = en.getValue
          result.putIfAbsent(key.toLowerCase(), value)
        }
        result
      }).map(result=>{
      try{
        val eqpId = result.get("eqpid").toString
        val putTime = result.get("newtime").toString

        val site = eqpId.substring(0, 2)
        val factory = Sites.toFactoryId(site)
        val rawString = eqpId + "|" + putTime
        val rowKey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString
        val rawLongTime: Long = Dates.string2Long(putTime, Dates.fmt2)
        val day_date: String = Dates.long2String(rawLongTime - 8 * 60 * 60 * 1000, Dates.fmt5)

        result.putIfAbsent("row_key", rowKey)
        result.putIfAbsent("site", site)
        result.putIfAbsent("factory", factory)
        result.putIfAbsent("day_date", day_date)
        result.putIfAbsent("shift", Dates.toShift(putTime, Dates.fmt2, site))
        result.putIfAbsent("long_time", (rawLongTime / 1000).toString)
        result.putIfAbsent("create_time", Dates.now(Dates.fmt2))

        result

      }catch {
        case e: Exception => {
          logger.error(result)
          null
        }
      }
    }).filter(_ != null)


    if(!"prod".equals(c.runMode)){
      kafkaSource.print()
    }
    kafkaSource.addSink(new SimpleHBaseTableSink(Builder.me().build(), c.tableName))
  }


}





