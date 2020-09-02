package com.aikosolar.bigdata.flink.job

import java.util

import com.aikosolar.bigdata.flink.common.utils.Dates
import com.aikosolar.bigdata.flink.job.conf.GpArgsConf
import com.alibaba.fastjson.JSON
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.util.hashing.MurmurHash3

/**
  * --job-name=GpArgsJob
  * --time-characteristic=EventTime
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --topic=data-collection-gp-subscription
  * --group.id=carlc4Test1
  * --checkpointDataUri=hdfs://172.16.98.85:8020/flink-checkpoint
  * --reset.strategy=earliest
  *
  * ods:ods_f_eqp_gp_mtlevtdata_cls
  *
  * @author carlc
  */
object GpArgsJob extends FLinkKafkaRunner[GpArgsConf] {

  val logger: Logger = Logger.getLogger(GpArgsJob.getClass)

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: GpArgsConf, rawKafkaSource: DataStream[String]): Unit = {
    val tags: Seq[String] = Seq("dabing", "fenchen", "qiliu", "fabai", "fahong", "special color", "too bright", "too dark")
    val fmtStream: KeyedStream[GP, String] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(x => (StringUtils.substringAfter(x.getString("EquipmentID"), "-"),
        StringUtils.removePattern(x.getString("EqMtlId"), " +\\S+"),
        MapUtils.getString(x, "Cls_CellQ", ""),
        MapUtils.getString(x, "Cls_ColQ", ""),
        x.getString("Come"))
      )
      .filter(x => StringUtils.isNotBlank(x._1)
        && StringUtils.isNotBlank(x._2)
        && StringUtils.isNotBlank(x._3)
        && StringUtils.isNotBlank(x._4)
        && StringUtils.isNotBlank(x._5)
      )
      .filter(x => x._2.contains("-"))
      .filter(x => tags.contains(x._3.toLowerCase()) || tags.contains(x._4.toLowerCase))
      .map(x => {
        val eqMtlId: String = x._2.trim
        if (eqMtlId.length == 24 || eqMtlId.length == 25) {
          val ix = eqMtlId.indexOf("-")
          val tube_id = eqMtlId.substring(ix - 4, ix + 2)
          val boat_id = eqMtlId.substring(ix + 2, ix + 7)
          val offset = if (eqMtlId.length == 24) 0 else 1
          val area = eqMtlId.substring(ix + 7, ix + 8 + offset)
          val site = eqMtlId.substring(ix + 8 + offset).replace("#", "")
          GP(x._1, tube_id, boat_id, area, site, x._3, x._4, x._5)
        } else {
          null
        }
      })
      .filter(x => x != null)
      .partitionCustom(new Partitioner[String] {
        override def partition(key: String, numPartitions: Int): Int = Math.abs(MurmurHash3.stringHash(key)) % numPartitions
      }, "boat_id")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[GP](Time.minutes(1)) {
        override def extractTimestamp(element: GP): Long = Dates.string2Long(element.putTime, Dates.fmt2)
      })
      .keyBy(x => x.boat_id)

    if (!"prod".equalsIgnoreCase(c.runMode)) {
      fmtStream.print("格式化")
    }

    fmtStream.process(new KeyedProcessFunction[String, GP, (GP, util.Map[String, Int])] {
      var accState: MapState[String, Int] = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val stateDescriptor: MapStateDescriptor[String, Int] = new MapStateDescriptor[String, Int]("acc-state", classOf[String], classOf[Int])
        val ttlConfig = StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.hours(2))
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .cleanupInBackground()
          .build
        stateDescriptor.enableTimeToLive(ttlConfig)
        accState = getRuntimeContext.getMapState(stateDescriptor)
      }

      override def processElement(value: GP, ctx: KeyedProcessFunction[String, GP, (GP, util.Map[String, Int])]#Context, out: Collector[(GP, util.Map[String, Int])]): Unit = {
        // 初始化状态
        if (accState.keys().size != 8) {
          accState.put("Dabing", 0)
          accState.put("fenchen", 0)
          accState.put("qiliu", 0)
          accState.put("Fabai", 0)
          accState.put("FaHong", 0)
          accState.put("special color", 0)
          accState.put("too bright", 0)
          accState.put("too dark", 0)
        }

        // 判断是否需要发送数据
        var sendFlag: Boolean = false
        if (accState.contains(value.cellQ)) {
          val v: Int = accState.get(value.cellQ) + 1
          accState.put(value.cellQ, v)
          if (!sendFlag) {
            sendFlag = v == 10
          }
        }
        if (accState.contains(value.colQ)) {
          val v = accState.get(value.colQ) + 1
          accState.put(value.colQ, v)
          if (!sendFlag) {
            sendFlag = v == 10
          }
        }

        if (sendFlag) {
          val r: util.Map[String, Int] = new util.LinkedHashMap[String, Int]()
          for (en <- accState.entries()) {
            r.put(en.getKey, en.getValue)
          }
          out.collect((value, r))
        }
      }
    }).printToErr("最终结果")

    //    val resultStream = fmtStream.timeWindow(Time.minutes(1))
    //      .trigger(new MyTrigger)
    //      .aggregate(new PreAggregator, new WindowFunc)
    //    resultStream.printToErr("最终结果")
  }


  //  class PreAggregator extends AggregateFunction[GP, (GP, util.Map[String, Int]), (GP, util.Map[String, Int])] {
  //    override def createAccumulator(): (GP, Map[String, Int]) = {
  //      val map: Map[String, Int] = new HashMap[String, Int]()
  //      map.put("dabing", 0)
  //      map.put("fenchen", 0)
  //      map.put("qiliu", 0)
  //      map.put("fabai", 0)
  //      map.put("fahong", 0)
  //      map.put("special color", 0)
  //      map.put("too bright", 0)
  //      map.put("too dark", 0)
  //      (null, map)
  //    }
  //
  //    override def add(value: GP, acc: (GP, util.Map[String, Int])): (GP, util.Map[String, Int]) = {
  //      if (acc._2.containsKey(value.cellQ.toLowerCase()))
  //        acc._2.put(value.cellQ.toLowerCase(), acc._2.get(value.cellQ.toLowerCase()) + 1)
  //      if (acc._2.containsKey(value.colQ.toLowerCase()))
  //        acc._2.put(value.colQ.toLowerCase(), acc._2.get(value.colQ.toLowerCase()) + 1)
  //      (value, acc._2)
  //    }
  //
  //    override def getResult(acc: (GP, util.Map[String, Int])): (GP, Map[String, Int]) = {
  //      acc
  //    }
  //
  //    override def merge(acc1: (GP, util.Map[String, Int]), acc2: (GP, util.Map[String, Int])): (GP, util.Map[String, Int]) = {
  //      acc2._2.forEach(new BiConsumer[String, Int] {
  //        override def accept(k: String, v: Int): Unit = {
  //          acc1._2.put(k, acc1._2.get(k) + v)
  //        }
  //      })
  //      acc1
  //    }
  //  }
  //
  //  class WindowFunc extends WindowFunction[(GP, util.Map[String, Int]), (GP, util.Map[String, Int]), String, TimeWindow] {
  //    override def apply(key: String, window: TimeWindow, input: Iterable[(GP, util.Map[String, Int])], out: Collector[(GP, util.Map[String, Int])]): Unit = {
  //      for (e <- input) {
  //        out.collect(e)
  //      }
  //    }
  //  }

  case class GP(eqp_id: String, tube_id: String, boat_id: String,
                area: String, site: String,
                cellQ: String, colQ: String, putTime: String)

}
