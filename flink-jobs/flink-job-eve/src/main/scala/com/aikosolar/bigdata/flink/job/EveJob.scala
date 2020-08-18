package com.aikosolar.bigdata.flink.job

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.enums.Sites
import com.aikosolar.bigdata.flink.common.utils.{Dates, IOUtils, Strings}
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.connectors.hbase.{HBaseOperation, HBaseSink}
import com.aikosolar.bigdata.flink.job.conf.EveConfig
import com.aikosolar.bigdata.flink.job.constants.Fileds
import com.aikosolar.bigdata.flink.job.enums.EveStep
import com.alibaba.fastjson.JSON
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

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
  * --mysql.url=jdbc:mysql://172.16.98.88:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF-8
  * --mysql.username=root
  * --mysql.password=123456
  * --mysql.sql="select * from eve_st_settings"
  * ----------------------字段映射----------------------
  * -f=currstep=_text1_
  * -f=text1=_text1_
  * -f=states=_states_
  * -f=status=_states_
  *
  * @author carlc
  */
object EveJob extends FLinkKafkaRunner[EveConfig] {

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: EveConfig, rawKafkaSource: DataStream[String]): Unit = {
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
        val eqpId: String = MapUtils.getString(x, "eqpid", "") // OK
        val tubeId: String = MapUtils.getString(x, "tubeid", "") // OK
        val states: String = MapUtils.getString(x, Fileds.STATES, "") // OK
        val putTime: String = MapUtils.getString(x, "puttime", "") // OK
        val runCount: String = MapUtils.getString(x, "runcount", "") // OK
        val eqpType = StringUtils.removePattern(eqpId.split("-")(1), "\\d+") // OK
        val rawString = eqpId + "|" + putTime
        val rowkey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString // OK
        val site = eqpId.substring(0, 2) // OK
        val factory = Sites.toFactoryId(site) // OK
        val shift = Dates.toShift(putTime, Dates.fmt2, site) // OK
        val day_date = Dates.long2String(Dates.string2Long(putTime, Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5) // OK
        val createTime = Dates.now(Dates.fmt2)
        val tagService = EveTagServiceFactory.getEveTagService(eqpType)
        val tag = if (tagService == null) null else { // OK
          val enumTag = tagService.tag(MapUtils.getString(x, Fileds.TEXT1, ""))
          if (enumTag == null) null else enumTag.toString
        }
        new Subscription(rowkey,
          factory,
          site,
          eqpId,
          eqpType,
          day_date,
          shift,
          putTime,
          tubeId,
          tag,
          createTime,
          runCount,
          states)
      })
      .filter(_.tag != null)
      .keyBy(x => (x.eqpId, x.tubeId))
      .process(new EveFunction())
      .map(new JoinMap)

    dateStream.print("结果")

    dateStream.addSink(new HBaseSink[Subscription](Builder.me().build(), c.tableName, new HBaseMutationConverter[Subscription] {
      override def insert(data: Subscription): Put = {
        val put: Put = new Put(Bytes.toBytes(data.rowkey))
        addColumn(put, "factory", data.factory)
        addColumn(put, "site", data.site)
        addColumn(put, "eqp_id", data.eqpId)
        addColumn(put, "eqp_type", data.eqpType)
        addColumn(put, "day_date", data.dayDate)
        addColumn(put, "shift", data.shift)
        addColumn(put, "test_time", data.putTime)
        addColumn(put, "end_time", data.endTime)
        addColumn(put, "tube_id", data.tubeId)
        addColumn(put, "odl_step_name", data.tag.toString)
        addColumn(put, "step_name", data.step_name.toString)
        addColumn(put, "data_type", data.dataType)
        addColumn(put, "output_qty", "1")
        addColumn(put, "ct", data.ct)
        addColumn(put, "st", data.st)
        addColumn(put, "loss", data.loss)
        addColumn(put, "sertue", data.sertue)
        addColumn(put, "set_st", data.set_st)
        addColumn(put, "set_st_loss", data.set_st_loss)
        addColumn(put, "set_st_sertue", data.set_st_sertue)
        addColumn(put, "createTime", data.createTime)
        addColumn(put, "run_count", data.runCount)
        addColumn(put, "states", data.states)
        put
      }

      override def delete(record: Subscription): Delete = {
        null
      }
    }, HBaseOperation.INSERT))
  }

  def addColumn(put: Put, key: String, value: Any): Unit = {
    if (value != null) {
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(key), Bytes.toBytes(value.toString))
    }
  }

  case class Subscription(rowkey: String,
                          factory: String,
                          site: String,
                          eqpId: String,
                          eqpType: String,
                          dayDate: String,
                          shift: String,
                          putTime: String,
                          tubeId: String,
                          tag: String,
                          createTime: String,
                          runCount: String,
                          states: String,
                          var output_qty: String = "1", // 默认1
                          var endTime: String = null,
                          var step_name: String = null,
                          var dataType: String = null,
                          var ct: Long = 0L,
                          var st: Long = 0L,
                          var loss: Long = 0L,
                          var sertue: String = null,
                          var set_st: Long = 0L,
                          var set_st_loss: Long = 0L,
                          var set_st_sertue: String = null
                         )

  /** 维表数据结构 */
  case class EveStSetting(eqpId: String, tubeId: String, st: Long, setSt: Long)

  class EveFunction extends KeyedProcessFunction[(String, String), Subscription, Subscription] {
    lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[Subscription]("previous", classOf[Subscription]))

    //    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
    //      val previous = previousSubscription.value()
    //      previousSubscription.update(value)
    //      if (previous != null) {
    //        previous.dataType = if (EveStep.valueOf(previous.tag).next().toString.equals(value.tag)) "Y" else "N"
    //        previous.endTime = value.putTime
    //        previous.step_name = value.tag
    //        previous.ct = (Dates.string2Long(value.putTime, Dates.fmt2) - Dates.string2Long(previous.putTime, Dates.fmt2))/1000
    //        previous.createTime = Dates.now(Dates.fmt2)
    //        out.collect(previous)
    //      }
    //    }

    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
      val previous = previousSubscription.value()
      if (previous != null) {
        if (previous.tag.equals(value.tag)) { // 如果2条数据的tag相同，且当前数据时间 > 前数据时间 => 状态更新为当前数据
          if (previous.putTime.compareTo(value.putTime) < 0) {
            previousSubscription.update(value)
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
  }

  /**
    * 因为这里的维表数据良很小，所以直接采用全表拉取
    */
  class JoinMap extends RichMapFunction[Subscription, Subscription] {
    lazy val logger = LoggerFactory.getLogger(classOf[JoinMap])

    var executor: ScheduledExecutorService = null
    var cache: Map[(String, String), EveStSetting] = new HashMap[(String, String), EveStSetting]()

    var loadFlag: AtomicBoolean = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      loadFlag = new AtomicBoolean(false)
      executor = Executors.newScheduledThreadPool(1)
      executor.scheduleAtFixedRate(new Runnable() {
        override def run(): Unit = {
          try {
            if (loadFlag.compareAndSet(false, true)) {
              val temp: Map[(String, String), EveStSetting] = load
              logger.info("加载维表数据成功,缓存大小:" + temp.size())
              if (MapUtils.isNotEmpty(temp)) {
                cache = temp
              }
            }
          }
          catch {
            case e: Exception =>
              logger.error("加载维表数据失败,使用缓存,缓存大小:" + cache.size(), e)
          } finally {
            loadFlag.set(false)
          }
        }
      }, 0, 30, TimeUnit.MINUTES) // 30分加载一次
    }

    override def map(value: Subscription): Subscription = {
      while (loadFlag.get()) {
        Thread.sleep(100)
      }

      val r = cache.get((value.eqpId, value.tubeId))
      if (r != null) {
        value.st = r.st
        value.loss = value.ct - r.st
        value.sertue = if (value.loss < 10 * 60) "Function" else if (value.loss > 30 * 60) "Trouble" else "Jam"

        value.set_st = r.setSt
        value.set_st_loss = value.ct - r.setSt
        value.set_st_sertue = if (value.set_st_loss < 10 * 60) "Function" else if (value.set_st_loss > 30 * 60) "Trouble" else "Jam"
      } else {
        logger.warn(s"维表缺失数据:(${value.eqpId},${value.tubeId})")
      }
      value
    }

    def load(): Map[(String, String), EveStSetting] = {
      import java.sql.DriverManager

      val temp: Map[(String, String), EveStSetting] = new HashMap[(String, String), EveStSetting]()
      var connection: Connection = null
      var statement: PreparedStatement = null
      var rs: ResultSet = null
      val c = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[EveConfig]
      try {
        Class.forName("com.mysql.jdbc.Driver")
        connection = DriverManager.getConnection(c.mysqlUrl, c.mysqlUsername, c.mysqlPassword)
        statement = connection.prepareStatement(c.mysqlSQL)
        rs = statement.executeQuery
        while (rs.next) {
          val eqpId = rs.getString("eqpId")
          val tubeId = rs.getString("tubeId")
          val st = rs.getLong("st")
          val set_st = rs.getLong("setst")
          temp.put((eqpId, tubeId), new EveStSetting(eqpId, tubeId, st, set_st))
        }
      }
      finally {
        IOUtils.closeQuietly(rs)
        IOUtils.closeQuietly(statement)
        IOUtils.closeQuietly(connection)
      }
      temp
    }

    override def close(): Unit = {
      if (executor != null) {
        executor.shutdown()
      }
    }
  }

}
