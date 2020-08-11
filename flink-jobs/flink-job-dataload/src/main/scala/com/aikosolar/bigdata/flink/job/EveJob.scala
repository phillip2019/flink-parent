package com.aikosolar.bigdata.flink.job

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.common.utils.{Dates, IOUtils, Strings}
import com.aikosolar.bigdata.flink.job.conf.EveConfig
import com.aikosolar.bigdata.flink.job.enums.EveStep
import com.alibaba.fastjson.JSON
import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory


/**
  *
  * 设备价值效率任务
  *
  * @author carlc
  */
object EveJob extends FLinkKafkaWithTopicRunner[EveConfig] {

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: EveConfig, rawKafkaSource: DataStream[(String, String)]): Unit = {
    // todo watermark
    rawKafkaSource.map(_._2).print("原始")

    val dateStream: DataStream[Subscription] = rawKafkaSource
      .map(x => (c.topicMapping.getOrDefault(x._1, null), JSON.parseObject(x._2, classOf[Subscription])))
      .filter(_._1 != null)
      .filter(x => StringUtils.isNotBlank(x._2.tubeId) && Strings.isValidEqpId(x._2.eqpId))
      .map(x => {
        val data: Subscription = x._2
        val tagService = EveTagServiceFactory.getEveTagService(x._1)
        if (tagService != null) {
          data.tag = tagService.tag(data.text1)
        }
        data
      })
      .filter(_.tag != null)
      .keyBy(x => (x.eqpId, x.tubeId))
      .process(new EveFunction())
      .map(new JoinMap)
    dateStream.print("处理结果")

    // 2 种方法
    // 1. DataStream关联为维表
    // 2. 使用table/sql关联且写入也用table/sql
    //    dateStream.addSink()
  }

  case class Subscription(eqpId: String, tubeId: String, states: String,
                          text1: String, putTime: String, runTime: String,
                          runCount: String,
                          var tag: EveStep = null,
                          var dataType: String = null,
                          var endTime: String = null,
                          var ct: Long = 0L,
                          var createTime: String = null,
                          var st: Long = 0L,
                          var set_st: Long = 0L,
                          var loss: Long = 0L,
                          var sertue: String = null,
                          var set_st_loss: Long = 0L,
                          var set_st_sertue: String = null

                         )

  /** 维表数据结构 */
  case class DimSt(eqpId: String, tubeId: String, st: Long, setSt: Long)

  class EveFunction extends KeyedProcessFunction[(String, String), Subscription, Subscription] {
    lazy val previousSubscription = getRuntimeContext.getState(new ValueStateDescriptor[Subscription]("previous", classOf[Subscription]))

    override def processElement(value: Subscription, ctx: KeyedProcessFunction[(String, String), Subscription, Subscription]#Context, out: Collector[Subscription]): Unit = {
      val previous = previousSubscription.value()
      previousSubscription.update(value)
      if (previous != null) {
        previous.dataType = if (previous.tag.next() == value.tag) "Y" else "N"
        previous.endTime = previous.putTime
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

    val executor: ScheduledExecutorService = null
    var cache: Map[(String, String), DimSt] = new HashMap[(String, String), DimSt]()

    var loadFlag: AtomicBoolean = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      executor.scheduleAtFixedRate(new Runnable() {
        override def run(): Unit = {
          try {
            loadFlag.compareAndSet(false, true) {
              val temp: Map[(String, String), DimSt] = load
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
      }, 0, 10, TimeUnit.MINUTES) // 10分加载一次
    }

    override def map(value: Subscription): Subscription = {
      while (!loadFlag.get()) {}

      val r = cache.get((value.eqpId, value.tubeId))
      value.st = r.st
      value.set_st = r.setSt
      value.loss = value.ct - r.st
      value.sertue = if (value.loss < 10 * 1000 * 60) "Function" else if (value.loss > 30 * 1000 * 60) "Trouble" else "Jam"
      value.set_st_loss = value.ct - r.setSt
      value.set_st_sertue = if (value.loss < 10 * 1000 * 60) "Function" else if (value.loss > 30 * 1000 * 60) "Trouble" else "Jam"

      value
    }

    def load(): Map[(String, String), DimSt] = {
      // todo 从mysql中加载
      import java.sql.DriverManager

      val temp: Map[(String, String), DimSt] = new HashMap[(String, String), DimSt]()
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
          val setst = rs.getLong("setst")
          temp.put((eqpId, tubeId), new DimSt(eqpId, tubeId, st, setst))
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
