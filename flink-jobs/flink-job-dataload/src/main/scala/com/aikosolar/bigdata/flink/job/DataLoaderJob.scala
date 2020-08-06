package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.SimpleHBaseTableSink
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.conf.DataLoaderConf
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala._
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
  * 尝试着写一个通用(简单，无逻辑的)数据加载工具
  *
  * @author carlc
  */
object DataLoaderJob extends FLinkKafkaRunner[DataLoaderConf] {
  val logger: Logger = Logger.getLogger(DataLoaderJob.getClass)

  /**
    * --job-name=DataLoaderJob
    * --time-characteristic=ProcessingTime
    * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
    * --group.id=carlc4Test
    * --topic=data-collection-df
    * --reset.strategy=earliest
    * --hbase.table=ods:ods_f_eqp_df_status
    *
    * 下面的表示字段映射
    * -f=Tube1Text1=text1
    * -f=Tube2Text1=text1
    * -f=Tube3Text1=text1
    * -f=Tube4Text1=text1
    * -f=Tube5Text1=text1
    * -f=Tube6Text1=text1
    * -f=Tube7Text1=text1
    * -f=Tube8Text1=text1
    * -f=Tube9Text1=text1
    * -f=Tube10Text1=text1
    */
  override def run0(env: StreamExecutionEnvironment, c: DataLoaderConf, rawKafkaSource: DataStream[String]): Unit = {

    val kafkaSource: DataStream[Map[String, AnyRef]] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey, en.getKey).toLowerCase(), en.getValue)
        }
        result
      })
      .filter(m => Strings.isValidEqpId(m.get("eqpid")))
      .filter(m => {
        val v = m.get("puttime")
        v != null && StringUtils.isNotBlank(v.toString)
      })
      .map(result => {
        try {
          val eqpId = result.get("eqpid").toString
          val putTime = result.get("puttime").toString

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
        } catch {
          case e: Exception => {
            logger.error(result)
            null
          }
        }
      })
      .filter(_ != null)

    if (!"prod".equals(c.runMode)) {
      kafkaSource.print()
    }
    kafkaSource.addSink(new SimpleHBaseTableSink(Builder.me().build(), c.tableName))
  }
}


