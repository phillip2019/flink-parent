package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter
import com.aikosolar.bigdata.flink.connectors.hbase.utils.Puts
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.connectors.hbase.{HBaseOperation, HBaseSink}
import com.aikosolar.bigdata.flink.job.conf.BtEveConfig
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.collections.MapUtils
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

object BtOeeJob extends FLinkKafkaRunner[BtEveConfig] {
  /**
   * 业务方法[不需自己调用env.execute()]
   * */
  override def run0(env: StreamExecutionEnvironment, c: BtEveConfig, rawKafkaSource: DataStream[String]): Unit = {

    val dateStream: DataStream[BtSubscription] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey.toLowerCase, en.getKey.toLowerCase()), en.getValue)
        }
        result
      }).filter(x => Strings.isValidEqpId(x.get("eqpid")) && Strings.isValidDataTime(MapUtils.getString(x, "puttime", "")))
      .map(x => {
        val eqpId: String = MapUtils.getString(x, "eqpid", "")
        val modulename: String = MapUtils.getString(x, "modulename", "")
        val e10state: String = MapUtils.getString(x, "e10state", "")
        val statuscode: String = MapUtils.getString(x, "statuscode", "")
        val putTime: String = MapUtils.getString(x, "puttime", "")
        val rawString = eqpId + "|" + putTime
        val rowkey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString // OK
        val site = eqpId.substring(0, 2) // OK
        val factory = Sites.toFactoryId(site) // OK
        val shift = Dates.toShift(putTime, Dates.fmt2, site) // OK
        val day_date = Dates.long2String(Dates.string2Long(putTime, Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5) // OK
        val createTime = Dates.now(Dates.fmt2)
        val module_id = modulename match {
          case "SDR" => "1"
          case "PCS 1" => "2"
          case modulename if ("RINSE 1".equals(modulename) || "RINSE 2".equals(modulename)) => "2"
          case modulename if ("TEXTURE 1".equals(modulename) || "TEXTURE 2".equals(modulename)) => "3"
          case "HU|HF" => "4"
          case "DIO3|HU" => "5"
          case "HWD" => "6"
          case modulename if ("WAD 1".equals(modulename) || "WAD 2".equals(modulename) || "WAD 3".equals(modulename)) => "7"
          case "Unload Station" => "8"
          case _ => null
        }
        val end_time = ""
        BtSubscription(rowkey, factory, site, eqpId, day_date, shift, putTime, end_time, e10state, modulename, statuscode
          , module_id, createTime, 0)
      }).filter(_.module_id != null)
      .keyBy(x => (x.eqpId, x.modulename))
      .process(new BtprocessFunction())


    dateStream.process(new OutProcessFunction() )


    if (!"prod".equals(c.runMode)) {
      rawKafkaSource.print("rawKafkaSource")
      dateStream.print("dateStream")
    }

    dateStream.addSink(new HBaseSink[BtSubscription](Builder.me().build(), c.tableName, new HBaseMutationConverter[BtSubscription] {
      override def insert(data: BtSubscription): Put = {
        val put: Put = new Put(Bytes.toBytes(data.rowkey))
        Puts.addColumn(put, "factory", data.factory)
        Puts.addColumn(put, "site", data.site)
        Puts.addColumn(put, "eqp_id", data.eqpId)
        Puts.addColumn(put, "day_date", data.dayDate)
        Puts.addColumn(put, "shift", data.shift)
        Puts.addColumn(put, "put_time", data.putTime)
        Puts.addColumn(put, "end_time", data.endTime)
        Puts.addColumn(put, "e10_state", data.e10state)
        Puts.addColumn(put, "module_name", data.modulename)
        Puts.addColumn(put, "status_code", data.statuscode)
        Puts.addColumn(put, "module_id", data.module_id)
        Puts.addColumn(put, "create_time", data.createTime)
        Puts.addColumn(put, "ct", data.ct)
        put
      }

      override def delete(record: BtSubscription): Delete = {
        null
      }
    }, HBaseOperation.INSERT))
  }


  case class BtSubscription(rowkey: String,
                              factory: String,
                              site: String,
                              eqpId: String,
                              dayDate: String,
                              shift: String,
                              putTime: String,
                              var endTime: String,
                              e10state: String,
                              modulename: String,
                              statuscode: String,
                              module_id: String,
                              createTime: String,
                              var ct: Long)

  class OutProcessFunction  extends ProcessFunction [BtSubscription,BtSubscription]{
    override def processElement(value: BtSubscription, ctx: ProcessFunction[BtSubscription, BtSubscription]#Context, out: Collector[BtSubscription]): Unit = ???
  }

  class BtprocessFunction extends KeyedProcessFunction[(String, String), BtSubscription, BtSubscription] {
    lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[BtSubscription]("previous", classOf[BtSubscription]))

    override def processElement(value: BtSubscription, ctx: KeyedProcessFunction[(String, String), BtSubscription, BtSubscription]#Context, out: Collector[BtSubscription]): Unit = {
      val previous = previousSubscription.value()
      if (previous != null) {
        previousSubscription.update(value)
        if (!"Unload Station".equals(previous.modulename)) {
          previous.endTime = value.putTime
          previous.ct = (Dates.string2Long(value.putTime, Dates.fmt2) - Dates.string2Long(previous.putTime, Dates.fmt2)) / 1000
        }
        out.collect(previous)
      } else {
        previousSubscription.update(value)
      }
    }
  }




}

