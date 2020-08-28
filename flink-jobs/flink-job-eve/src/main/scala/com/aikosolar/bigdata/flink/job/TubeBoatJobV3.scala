package com.aikosolar.bigdata.flink.job

import java.util
import java.util.function.Function
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.SimpleHBaseTableSink
import com.aikosolar.bigdata.flink.connectors.hbase.utils.RowKeyGenerator
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.conf.TubeBoatJobConfig
import com.alibaba.fastjson.JSON
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
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
    val dateStream: DataStream[Map[String, AnyRef]] = rawKafkaSource.map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey.toLowerCase, en.getKey.toLowerCase()), en.getValue)
        }
        result
      })
      .filter(x => Strings.isValidEqpId(x.get("eqpid")) && Strings.isValidDataTime(MapUtils.getString(x, "puttime", "")))
      .keyBy(x =>
        (MapUtils.getString(x, "eqpid"),
          MapUtils.getString(x, "run_count1", ""),
          MapUtils.getString(x, "run_count2", ""),
          MapUtils.getString(x, "run_count3", ""),
          MapUtils.getString(x, "run_count4", ""),
          MapUtils.getString(x, "run_count5", ""))
      ).process(new KeyedProcessFunction[(String, String, String, String, String, String), Map[String, AnyRef], Map[String, AnyRef]] {
      var sendFlag: ValueState[Boolean] = _

      override def open(parameters: Configuration): Unit = {
        val state = new ValueStateDescriptor[Boolean]("send-flag-state", classOf[Boolean])
        val ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)) // 2 小时
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .cleanupInBackground()
          .build
        state.enableTimeToLive(ttlConfig)
        sendFlag = getRuntimeContext.getState(state)
      }

      override def processElement(value: util.Map[String, AnyRef], ctx: KeyedProcessFunction[(String, String, String, String, String, String), util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context, out: Collector[util.Map[String, AnyRef]]): Unit = {
        if (sendFlag.value() == null || !sendFlag.value()) {
          out.collect(value)
        }
      }

      override def close(): Unit = {
        sendFlag.clear()
      }
    }).flatMap(new FlatMapFunction[Map[String, AnyRef], Map[String, AnyRef]] {
      override def flatMap(x: util.Map[String, AnyRef], out: Collector[util.Map[String, AnyRef]]): Unit = {
        val EqpID = MapUtils.getString(x, "eqpid", "")
        val PutTime = MapUtils.getString(x, "puttime", "")
        val tmp = EqpID.split("-")
        val prefix = tmp(tmp.length - 1)
        for (id <- 1 to 12) {
          val Tube = MapUtils.getString(x, s"boat${id}.tube", "").trim
          if (StringUtils.isNotBlank(Tube) && Tube.startsWith(prefix + "-")) {
            val Recipe = MapUtils.getString(x, s"boat${id}.recipe", "").trim
            val CoolTime = MapUtils.getString(x, s"boat${id}.cooltime", "").trim
            val State = MapUtils.getString(x, s"boat${id}.state", "").trim
            val LoadState = MapUtils.getString(x, s"boat${id}.loadstate", "").trim
            val Position = MapUtils.getString(x, s"boat${id}.position", "").trim
            val LoadMode = MapUtils.getString(x, s"boat${id}.loadmode", "").trim
            val CoolTimeDefined = MapUtils.getString(x, s"boat${id}.cooltimedefined", "").trim
            val BoatID = MapUtils.getString(x, s"boat${id}.boatid", "").trim
            val BoatRuns = MapUtils.getString(x, s"boat${id}.boatruns", "").trim
            val RunCount = MapUtils.getString(x, s"tube${Tube.replace(prefix + "-", "")}.runcount", "").trim

            if (StringUtils.isNotBlank(RunCount) && StringUtils.isNotBlank(BoatID) && StringUtils.isNotBlank(LoadState)) {
              val data: Map[String, AnyRef] = new HashMap[String, AnyRef]()
              data.put("eqp_id", EqpID)
              data.put("put_time", PutTime)
              data.put("tube_id", Tube)
              data.put("recipe", Recipe)
              data.put("cool_time", CoolTime)
              data.put("state", State)
              data.put("load_state", LoadState)
              data.put("position", Position)
              data.put("load_mode", LoadMode)
              data.put("cool_time_defined", CoolTimeDefined)
              data.put("boat_id", BoatID)
              data.put("boat_runs", BoatRuns)
              data.put("run_count", RunCount)
              out.collect(data)
            }
          }
        }
      }
    })
      .map(x => {
        val site = MapUtils.getString(x, "eqp_id", "").substring(0, 2)
        val factory = Sites.toFactoryId(site)
        val shift = Dates.toShift(MapUtils.getString(x, "put_time", ""), Dates.fmt2, site)
        val day_date = Dates.long2String(Dates.string2Long(MapUtils.getString(x, "put_time", ""), Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5)
        val createTime = Dates.now(Dates.fmt2)
        val row_key = RowKeyGenerator.gen(null.asInstanceOf[Function[String, String]],
          MapUtils.getString(x, "tube_id"),
          MapUtils.getString(x, "boat_id"),
          MapUtils.getString(x, "run_count"),
          MapUtils.getString(x, "load_state") match {
            case "Loading" => "1"
            case "Wait for Loading/Unload" => "2"
            case "Processing" => "3"
            case "Wait for process" => "4"
            case "Cooling time elapsed" => "5"
            case "Cooling" => "6"
            case _ => "0"
          },
          MapUtils.getString(x, "put_time")
        )
        x.put("site", site)
        x.put("factory", factory)
        x.put("shift", shift)
        x.put("day_date", day_date)
        x.put("create_Time", createTime)
        x.put("row_key", row_key)
        x
      })

    if (!"prod".equals(c.runMode)) {
      dateStream.print("结果")
    }

    dateStream.addSink(new SimpleHBaseTableSink(Builder.me().conf(c.hbaseConfig).build(), c.tableName))
  }

}
