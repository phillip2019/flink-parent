package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter
import com.aikosolar.bigdata.flink.connectors.hbase.utils.Puts
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.connectors.hbase.{HBaseOperation, HBaseSink}
import com.aikosolar.bigdata.flink.job.conf.EveV2Config
import com.aikosolar.bigdata.flink.job.constants.Fields
import com.aikosolar.bigdata.flink.job.enums.EveStep
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._


/**
  *
  * 设备价值效率任务
  *
  * --job-name=EveJob
  * --time-characteristic=ProcessingTime
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=carlc4Test
  * --topic=data-collection-(pe|pr|df|ta)-subscription
  * --is.topic.regex=true
  * --reset.strategy=earliest
  * --hbase.table=ods:ods_f_test_to_dai //todo 修改表名称
  * ----------------------字段映射----------------------
  * -f=currstep=_text1_
  * -f=text1=_text1_
  * -f=states=_states_
  * -f=status=_states_
  *
  * @author carlc
  */
object EveJobV2 extends FLinkKafkaRunner[EveV2Config] {


  /**
    * 构建/初始化 env
    */
  override def setupEnv(env: StreamExecutionEnvironment, c: EveV2Config): Unit = {
    super.setupEnv(env,c)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }

  /**
    * 配置校验
    */
  override def validate(c: EveV2Config): Unit = {
    if(c.topicRegex || c.topic.contains(",")){
      throw new UnsupportedOperationException("当前任务不支持批量topic")
    }
  }

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: EveV2Config, rawKafkaSource: DataStream[String]): Unit = {
    val dateStream: DataStream[Subscription] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey.toLowerCase, en.getKey.toLowerCase()), en.getValue)
        }
        result
      })
      .filter(x => Strings.isValidEqpId(x.get("eqpid")) && Strings.isValidDataTime(MapUtils.getString(x, "puttime", "")))
      .map(x => {
        val eqpId: String = MapUtils.getString(x, "eqpid", "")
        val tubeId: String = MapUtils.getString(x, "tubeid", "")
        val states: String = MapUtils.getString(x, Fields.STATES, "")
        val putTime: String = MapUtils.getString(x, "puttime", "")
        val runCount: String = MapUtils.getString(x, "runcount", "")
        val eqpType = StringUtils.removePattern(eqpId.split("-")(1), "\\d+")
        val rawString = eqpId + "|" + putTime
        val rowkey = if ("".equals(tubeId.trim)) DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString
        else DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString + "|" + tubeId

        val site = eqpId.substring(0, 2)
        val factory = Sites.toFactoryId(site)
        val shift = Dates.toShiftChar(putTime, Dates.fmt2, site)
        val day_date = Dates.long2String(Dates.string2Long(putTime, Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5)
        val createTime = Dates.now(Dates.fmt2)
        val year="Y"+day_date.substring(0,4)
        val month="M"+day_date.substring(5,7)
        val week="W"+Dates.getWeek(day_date,Dates.fmt5)
        val tagService = EveTagServiceFactory.getEveTagService(eqpType)
        val tag = if (tagService == null) null else {
          val enumTag = tagService.tag(MapUtils.getString(x, Fields.TEXT1, ""))
          if (enumTag == null) null else enumTag.toString
        }
        Subscription(rowkey,
          factory,
          site,
          eqpId,
          eqpType,
          day_date,
          shift,
          year,
          month,
          week,
          putTime,
          tubeId,
          tag,
          createTime,
          runCount,
          states)
      })
      .filter(_.tag != null)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Subscription] {
        private val MAX_LATENESS: Long = 2 * 60 * 1000
        private var CURRENT_TS = Long.MinValue
        override def checkAndGetNextWatermark(lastElement: Subscription, extractedTimestamp: Long): Watermark = {
          val ts = CURRENT_TS - MAX_LATENESS
          new Watermark(ts)
        }
        override def extractTimestamp(element: Subscription, previousElementTimestamp: Long): Long = try {
          val current = Dates.string2Long(element.putTime,Dates.fmt2)
          CURRENT_TS = CURRENT_TS.max(current)
          current
        }
      })
      .keyBy(x => (x.eqpId, x.tubeId))
      .process(new EveFunction())

    if (!"prod".equals(c.runMode)) {
      dateStream.print("结果")
    }

    dateStream.addSink(new HBaseSink[Subscription](Builder.me().build(), c.tableName, new HBaseMutationConverter[Subscription] {
      override def insert(data: Subscription): Put = {
        val put: Put = new Put(Bytes.toBytes(data.rowkey))
        Puts.addColumn(put, "factory", data.factory)
        Puts.addColumn(put, "site", data.site)
        Puts.addColumn(put, "eqp_id", data.eqpId)
        Puts.addColumn(put, "eqp_type", data.eqpType)
        Puts.addColumn(put, "day_date", data.dayDate)
        Puts.addColumn(put, "shift", data.shift)
        Puts.addColumn(put, "test_time", data.putTime)
        Puts.addColumn(put, "end_time", data.endTime)
        Puts.addColumn(put, "year", data.year)
        Puts.addColumn(put, "month", data.month)
        Puts.addColumn(put, "week", data.week)
        Puts.addColumn(put, "tube_id", data.tubeId)
        Puts.addColumn(put, "odl_step_name", data.tag.toString)
        Puts.addColumn(put, "step_name", data.step_name.toString)
        Puts.addColumn(put, "data_type", data.dataType)
        Puts.addColumn(put, "ct", data.ct)
        Puts.addColumn(put, "createTime", data.createTime)
        Puts.addColumn(put, "run_count", data.runCount)
        Puts.addColumn(put, "states", data.states)
        put
      }

      override def delete(record: Subscription): Delete = {
        null
      }
    }, HBaseOperation.INSERT))
  }

  case class Subscription(rowkey: String,
                          factory: String,
                          site: String,
                          eqpId: String,
                          eqpType: String,
                          dayDate: String,
                          shift: String,
                          year:String,
                          month:String,
                          week:String,
                          putTime: String,
                          tubeId: String,
                          tag: String,
                          createTime: String,
                          runCount: String,
                          states: String,
                          var endTime: String = null,
                          var step_name: String = null,
                          var dataType: String = null,
                          var ct: Long = 0L
                         )

  /** 维表数据结构 */
  case class EveStSetting(eqpId: String, tubeId: String, st: Long, setSt: Long)

  class EveFunction extends KeyedProcessFunction[(String, String), Subscription, Subscription] {
    lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[Subscription]("previous", classOf[Subscription]))

    lazy val shiftTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("Shift-Timer", classOf[Long]))

    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
      val timer = Dates.toSwitchShiftTime(value.putTime, Dates.fmt2, value.site)
      if (timer != -1L && (shiftTimer.value() == null || timer != shiftTimer.value())) {
        shiftTimer.update(timer)
        ctx.timerService().registerEventTimeTimer(timer)
      }

      val previous = previousSubscription.value()
      if (previous != null) {
        if (previous.tag.equals(value.tag)) { // 如果2条数据的tag相同，且当前数据时间 > 前数据时间 => 状态更新为当前数据
          if (previous.eqpType.equals("DF")) {
            if ("CONDITION".equals(previous.tag) || "CLEAN".equals(previous.tag)) {
              if (previous.putTime.compareTo(value.putTime) > 0) {
                previousSubscription.update(value)
              }
            }
          } else {
            if (previous.putTime.compareTo(value.putTime) < 0) {
              previousSubscription.update(value)
            }
          }
        } else {
          previousSubscription.update(value)
          previous.endTime = value.putTime
          previous.step_name = value.tag
          previous.dataType = if (EveStep.valueOf(previous.tag).next(value.eqpType).toString.equals(value.tag)) "Y" else "N"
          previous.ct = (Dates.string2Long(value.putTime, Dates.fmt2) - Dates.string2Long(previous.putTime, Dates.fmt2)) / 1000
          out.collect(previous)
        }
      } else {
        previousSubscription.update(value)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#OnTimerContext, out: Collector[Subscription]): Unit = {
      val previous = previousSubscription.value()
      val timer = shiftTimer.value()
      if (previous != null && timestamp == timer) {
        previous.endTime = Dates.long2String(timer, Dates.fmt2)
        previous.step_name = EveStep.valueOf(previous.tag).next(previous.site).toString
        previous.dataType = "Y"
        previous.ct = (timer - Dates.string2Long(previous.putTime, Dates.fmt2)) / 1000
        out.collect(previous)

        // 注册下一个定时器
        ctx.timerService().deleteEventTimeTimer(timer)
        ctx.timerService().registerEventTimeTimer(Dates.toSwitchShiftTime(Dates.long2String(timer, Dates.fmt2), Dates.fmt2, previous.site))
      }
    }
  }

}
