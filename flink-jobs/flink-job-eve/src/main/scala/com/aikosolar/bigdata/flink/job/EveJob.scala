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


/**
  *
  * 设备价值效率任务
  *
  * --job-name=DataLoaderJob
  * --time-characteristic=ProcessingTime
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092
  * --group.id=carlc4Test
  * --topic=data-collection-pe-subscription
  * --reset.strategy=earliest
  * --hbase.table=todo-xxxx
  * --mysql.url="jdbc:mysql://172.16.98.88:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF-8"
  * --mysql.username=root
  * --mysql.password=123456
  * --mysql.sql=select * from eve_st_settings
  * --topic.mapping=data-collection-pr-subscription=PE
  *
  * @author carlc
  */
object EveJob extends FLinkKafkaWithTopicRunner[EveConfig] {

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: EveConfig, rawKafkaSource: DataStream[(String, String)]): Unit = {
    val dateStream: DataStream[Subscription] = rawKafkaSource
      .map(x => {
        (c.topicMapping.getOrDefault(x._1, null), JSON.parseObject(x._2, classOf[Subscription]))
      })
      .filter(_._1 != null)
      .filter(x => StringUtils.isNotBlank(x._2.tubeId) && Strings.isValidEqpId(x._2.eqpId) && StringUtils.isNoneBlank(x._2.putTime))
      .map(x => {
        val data: Subscription = x._2
        data.eqp_type = data.eqpId.split("-")(1).replaceAll("\\d+", "")
        val tagService = EveTagServiceFactory.getEveTagService(x._1)
        if (tagService != null) {
          // 兼容TA的取值方式
          data.tag = if ("ta".equalsIgnoreCase(data.eqp_type)) tagService.tag(data.CurrStep) else tagService.tag(data.text1)
          val rawString = data.eqpId + "|" + data.putTime
          data.site = data.eqpId.substring(0, 2)
          data.factory = Sites.toFactoryId(data.site)
          data.rowkey = DigestUtils.md5Hex(rawString).substring(0, 2) + "|" + rawString
          data.day_date = Dates.long2String(Dates.string2Long(data.putTime, Dates.fmt2) - 8 * 60 * 60 * 1000, Dates.fmt5)
        }
        data
      })
      .filter(_.tag != null)
      //      可以用processTime,没必要分配水位
      //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Subscription](Time.minutes(5)) {
      //        override def extractTimestamp(v: Subscription): Long = Dates.string2Long(v.putTime, Dates.fmt2)
      //      })
      .keyBy(x => (x.eqpId, x.tubeId))
      .process(new EveFunction())
      .map(new JoinMap)

    dateStream.addSink(new HBaseSink[Subscription](Builder.me().build(), c.tableName, new HBaseMutationConverter[Subscription] {
      override def insert(data: Subscription): Put = {
        val put: Put = new Put(Bytes.toBytes(data.rowkey))
        addColumn(put, "factory", data.factory)
        addColumn(put, "site", data.site)
        addColumn(put, "eqp_id", data.eqpId)
        addColumn(put, "eqp_type", data.eqp_type)
        addColumn(put, "day_date", data.day_date)
        addColumn(put, "shift", data.shift)
        addColumn(put, "test_time", data.putTime)
        addColumn(put, "end_time", data.endTime)
        addColumn(put, "tube_id", data.tubeId)
        addColumn(put, "odl_step_name", data.tag.toString)
        addColumn(put, "step_name", data.step_name.toString)
        addColumn(put, "data_type", data.dataType)
        addColumn(put, "output_qty", data.output_qty)
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
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("factory"), Bytes.toBytes(value.toString))
    }
  }

  case class Subscription(eqpId: String, tubeId: String, states: String,
                          text1: String, putTime: String,
                          runCount: String,
                          CurrStep: String,
                          var output_qty: String = "1",
                          var tag: EveStep = null,
                          var dataType: String = null,
                          var endTime: String = null,
                          var createTime: String = null,
                          var ct: Long = 0L,
                          var step_name: EveStep = null,
                          var st: Long = 0L,
                          var loss: Long = 0L,
                          var sertue: String = null,

                          var set_st: Long = 0L,
                          var set_st_loss: Long = 0L,
                          var set_st_sertue: String = null,

                          var rowkey: String = null,
                          var factory: String = null,
                          var site: String = null,
                          var eqp_type: String = null,
                          var day_date: String = null,
                          var shift: String = null
                         )

  /** 维表数据结构 */
  case class EveStSetting(eqpId: String, tubeId: String, st: Long, setSt: Long)

  class EveFunction extends KeyedProcessFunction[(String, String), Subscription, Subscription] {
    lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[Subscription]("previous", classOf[Subscription]))

    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
      val previous = previousSubscription.value()
      previousSubscription.update(value)
      if (previous != null) {
        previous.dataType = if (previous.tag.next() == value.tag) "Y" else "N"
        previous.endTime = previous.putTime
        previous.step_name = value.tag
        previous.ct = Dates.string2Long(value.putTime, Dates.fmt2) - Dates.string2Long(previous.putTime, Dates.fmt2)
        previous.createTime = Dates.now(Dates.fmt2)
        out.collect(previous)
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
        value.sertue = if (value.loss < 10 * 1000 * 60) "Function" else if (value.loss > 30 * 1000 * 60) "Trouble" else "Jam"

        value.set_st = r.setSt
        value.set_st_loss = value.ct - r.setSt
        value.set_st_sertue = if (value.set_st_loss < 10 * 1000 * 60) "Function" else if (value.set_st_loss > 30 * 1000 * 60) "Trouble" else "Jam"
      } else {
        logger.warn("维表缺失数据:({},{})", value.eqpId, value.tubeId)
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
