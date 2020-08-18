package com.aikosolar.bigdata.flink.job

import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.{HBaseOperation, HBaseSink}
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.BtHfOeeJob.BtHfSubscription
import com.aikosolar.bigdata.flink.job.EveJob.{EveFunction, Subscription, addColumn}
import com.aikosolar.bigdata.flink.job.conf.{BtHfEveConfig, EveConfig}
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.collections.MapUtils
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes


/* *
  * 设备价值效率任务
  *
  * --job-name=BtHfOeeJob
  * --time-characteristic=ProcessingTime
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=carlc4Test
  * --topic=data-collection-bt-subscription
  * --reset.strategy=latest
  * --hbase.table=ods:ods_f_eqp_chain_oee //todo 修改表名称
  * */
import scala.collection.JavaConversions._
object BtHfOeeJob extends FLinkKafkaRunner[BtHfEveConfig]{
  /**
   * 业务方法[不需自己调用env.execute()]
   */
  override def run0(env: StreamExecutionEnvironment, c: BtHfEveConfig, rawKafkaSource: DataStream[String]): Unit = {
    var dateStream: DataStream[BtHfSubscription] = rawKafkaSource
      .map(JSON.parseObject(_))
      .map(jsonObj => {
        val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
        for (en <- jsonObj.entrySet) {
          result.putIfAbsent(c.fieldMapping.getOrDefault(en.getKey.toLowerCase, en.getKey.toLowerCase()), en.getValue)
        }
        result
      }).filter(x => Strings.isValidEqpId(x.get("eqpid")) && Strings.isValidDataTime(MapUtils.getString(x, "puttime", "")))
      .map(x=>{
        val eqpId: String = MapUtils.getString(x, "eqpid", "")
        val modulename: String = MapUtils.getString(x, "modulename", "")
        val e10state: String =  MapUtils.getString(x, "e10state", "")
        val statuscode: String =  MapUtils.getString(x, "statuscode", "")
        val putTime: String =  MapUtils.getString(x, "puttime", "")
        val rawString = eqpId + "|" + putTime
        val rowkey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString // OK
        val site = eqpId.substring(0, 2) // OK
        val factory = Sites.toFactoryId(site) // OK
        val shift = Dates.toShift(putTime, Dates.fmt2, site) // OK
        val day_date = Dates.long2String(Dates.string2Long(putTime, Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5) // OK
        val createTime = Dates.now(Dates.fmt2)
        val module_id= modulename match {
          case "SDR" => "1"
          case "PCS 1" => "2"
          case modulename if("RINSE 1".equals(modulename)  || "RINSE 2".equals(modulename))=> "2"
          case modulename if("TEXTURE 1".equals(modulename)  || "TEXTURE 2".equals(modulename))=> "3"
          case "HU|HF" => "4"
          case "DIO3|HU" => "5"
          case "HWD" => "6"
          case modulename if("WAD 1".equals(modulename)  || "WAD 2".equals(modulename) || "WAD 3".equals(modulename))=> "7"
          case "Unload Station" => "8"
          case _ => null
        }
        val end_time=""
        BtHfSubscription(rowkey,factory,site,eqpId,day_date,shift,putTime,end_time,e10state,modulename,statuscode
          ,module_id,createTime,0)
      }).filter(_.module_id !=null)
      .keyBy(x => (x.eqpId, x.modulename))
      .process(new BtHfprocessFunction())
    rawKafkaSource.print("rawKafkaSource")
    dateStream.print("dateStream")
    dateStream.addSink(new HBaseSink[BtHfSubscription](Builder.me().build(), c.tableName, new HBaseMutationConverter[BtHfSubscription] {
      override def insert(data: BtHfSubscription): Put = {
        val put: Put = new Put(Bytes.toBytes(data.rowkey))
        addColumn(put, "factory", data.factory)
        addColumn(put, "site", data.site)
        addColumn(put, "eqp_id", data.eqpId)
        addColumn(put, "day_date", data.dayDate)
        addColumn(put, "shift", data.shift)
        addColumn(put, "put_time", data.putTime)
        addColumn(put, "end_time", data.endTime)
        addColumn(put, "e10_state", data.e10state)
        addColumn(put, "module_name", data.modulename)
        addColumn(put, "status_code", data.statuscode)
        addColumn(put, "module_id", data.module_id)
        addColumn(put, "create_time", data.createTime)
        addColumn(put, "ct", data.ct)
        put
      }

      override def delete(record: BtHfSubscription): Delete = {
        null
      }
    }, HBaseOperation.INSERT))
  }

  def addColumn(put: Put, key: String, value: Any): Unit = {
    if (value != null) {
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(key), Bytes.toBytes(value.toString))
    }
  }



  case class BtHfSubscription (rowkey: String,
                               factory: String,
                               site: String,
                               eqpId: String,
                               dayDate: String,
                               shift: String,
                               putTime: String,
                               var endTime: String,
                               e10state:String,
                               modulename:String,
                               statuscode :String,
                               module_id:String,
                               createTime:String,
                               var  ct:Long)
}


class BtHfprocessFunction  extends KeyedProcessFunction[(String, String), BtHfSubscription, BtHfSubscription] {
  lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[BtHfSubscription]("previous", classOf[BtHfSubscription]))

  override def processElement(value: BtHfSubscription, ctx: KeyedProcessFunction[(String, String), BtHfSubscription, BtHfSubscription]#Context, out: Collector[BtHfSubscription]): Unit = {
    val previous = previousSubscription.value()
   if (previous != null) {
     previousSubscription.update(value)
      if(!"Unload Station".equals(previous.modulename)){
        previous.endTime=value.putTime
        previous.ct=(Dates.string2Long(value.putTime, Dates.fmt2) - Dates.string2Long(previous.putTime, Dates.fmt2)) / 1000
      }
      out.collect(previous)
    }else {
      previousSubscription.update(value)
    }
  }
}
