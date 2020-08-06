package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.constants.Constants
import com.aikosolar.bigdata.flink.connectors.hbase.{DynamicHBaseSink, SimpleHBaseTableSink}
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.DataLoaderJob.logger
import com.aikosolar.bigdata.flink.job.conf.{DataLoaderConf, DataLoaderWithTopicsConf}
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala._
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
  * 尝试着写一个通用(简单，无逻辑的)数据加载工具
  *
  */
object DataLoaderWithTopicsJob extends FLinkKafkaWithTopicRunner[DataLoaderWithTopicsConf] {
  val logger:Logger= Logger.getLogger(DataLoaderWithTopicsJob.getClass)
 /*--job-name=test
  *--bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  *--group.id=test
  *--reset.strategy=groupoffsets
  *--topic=data-collection-pe-subscription,data-collection-pr-subscription,data-collection-df-subscription
  *-tableMapping=data-collection-df-subscription=ods:ods_f_test_to_dai
  *-tableMapping=data-collection-pr-subscription=ods:ods_f_test_to_dai22
  *-tableMapping=data-collection-pe-subscription=ods:ods_f_test_to_dai22
*/
   /**
   * 业务方法[不需自己调用env.execute()]
   */

  override def run0(env: StreamExecutionEnvironment, c: DataLoaderWithTopicsConf, rawKafkaSource: DataStream[(String, String)]): Unit = {
    val kafkaSource:DataStream[Map[String, AnyRef]]=rawKafkaSource
      .map(line=>(line._1,JSON.parseObject(line._2)))
          .map(line=> {
            var result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
            val topic = line._1
            result.putIfAbsent(Constants.topic, topic)
            println(line)
            for (en <- line._2.entrySet) {
              val key = c.fieldMapping.getOrDefault(en.getKey, en.getKey)
              val value = en.getValue
              result.putIfAbsent(key.toLowerCase(), value)
            }
            result
          })
      .filter(m => Strings.isValidEqpId(m.get("eqpid")))
      .filter(m => {
        val v = m.get("puttime")
        v != null && StringUtils.isNotBlank(v.toString)
      }).map(result=> {
      try {
          val eqpId = result.get("eqpid").toString
          val putTime = result.get("puttime").toString

          val site = eqpId.substring(0, 2)
          val factory = Sites.toFactoryId(site)
          val rawString = eqpId + "|" + putTime
          val rowKey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString
          val rawLongTime: Long = Dates.string2Long(putTime, Dates.fmt2)
          val day_date: String = Dates.long2String(rawLongTime - 8 * 60 * 60 * 1000, Dates.fmt5)

          result.putIfAbsent(Constants.ROW_KEY, rowKey)
          result.putIfAbsent("site", site)
          result.putIfAbsent("factory", factory)
          result.putIfAbsent("day_date", day_date)
          result.putIfAbsent("shift", Dates.toShift(putTime, Dates.fmt2, site))
          result.putIfAbsent("long_time", (rawLongTime / 1000).toString)
          result.putIfAbsent("create_time", Dates.now(Dates.fmt2))
          result
        }
        catch
        {
          case e: Exception => {
            logger.error(result)
            null
          }
        }
    }).filter(_ != null)

    if(!"prod".equals(c.runMode)){
      kafkaSource.print()
    }

    val familyMapping: Map[String, String] = new HashMap[String, String]()
    familyMapping.put(Constants.DEFAULT_FAMILY_KEY, "cf")
    kafkaSource.addSink(new DynamicHBaseSink(Builder.me().build(), c.tableMapping, familyMapping))

  }
}


