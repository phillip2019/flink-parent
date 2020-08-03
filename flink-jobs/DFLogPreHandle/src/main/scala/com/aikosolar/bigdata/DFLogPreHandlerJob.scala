package com.aikosolar.bigdata

import java.util.Properties

import com.aikosolar.app.conf.DFLogPreHandleConfig
import com.aikosolar.bigdata.flink.job.FLinkKafkaRunner
import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import com.alibaba.fastjson.JSONPath
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
  *
  * DF日志数据预处理器
  *
  * input: kafka
  *
  * output: kafka
  *
  * 逻辑: 根据特定的开始、结束标志截取开始-结束之前的数据，合并后发送到目标kafka topic,不做其他业务逻辑处理(由下游进行逻辑加工)
  *
  * 运行参数:
  *
  * --job-name=df-log-job
  * --time-characteristic=EventTime
  * --bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
  * --topic=t1
  * --group.id=g1
  * --target.bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
  * --target.topic=t2
  * --target.miss.begin.topic=t3
  * --target.miss.end.topic=t4
  * --checkpointDataUri=hdfs://172.16.98.85:8020/flink-checkpoint
  *
  * @author carlc
  */
object DFLogPreHandlerJob extends FLinkKafkaRunner[DFLogPreHandleConfig] {
  lazy val L: Logger = LoggerFactory.getLogger(DFLogPreHandlerJob.getClass)

  override def run0(env: StreamExecutionEnvironment, c: DFLogPreHandleConfig, rawKafkaSource: DataStream[String]): Unit = {

    val stream = rawKafkaSource
      .filter(StringUtils.isNotBlank(_))
      .map(x => {
        val hostname = JSONPath.eval(x,"$.host.name").toString
        val path = JSONPath.eval(x,"$.log.file.path").toString
        val message = JSONPath.eval(x,"$.message").toString
        DeviceLog(hostname, path, message)
      })
      .map(_.message)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String] {

        private val MAX_LATENESS: Long = 2 * 60 * 1000

        private var CURRENT_TS = Long.MinValue

        override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
          val ts = CURRENT_TS - MAX_LATENESS
          L.info("当前水位:{}", ts)
          new Watermark(ts)
        }

        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = try {
          val current = DateUtils.parseDate(element.substring(0, 19), "MM/dd/yyyy HH:mm:ss").getTime
          CURRENT_TS = CURRENT_TS.max(current)
          L.info("当前时间戳:{}", current)
          current
        }
        catch {
          case e: Exception =>
            return previousElementTimestamp
        }
      })
      .map(("dummyKey", _)) // 由于后续需要keyedState,所以这里添加一个无逻辑含义的字段
      .keyBy(_._1)
      .process(new MergeFunction())

    stream.addSink(new FlinkKafkaProducer010[String](c.targetBootstrapServers, c.targetTopic, new SimpleStringSchema()))

    if ("test".equalsIgnoreCase(c.runMode)) {
      stream.print()
    }

    val beginMissStream = stream.getSideOutput(new OutputTag[String]("miss-begin-stream"))
    beginMissStream.addSink(new FlinkKafkaProducer010[String](c.targetBootstrapServers, c.targetMissBeginTopic, new SimpleStringSchema()))

    if ("test".equalsIgnoreCase(c.runMode)) {
      beginMissStream.printToErr()
    }

    val endMissStream = stream.getSideOutput(new OutputTag[String]("miss-end-stream"))
    endMissStream.addSink(new FlinkKafkaProducer010[String](c.targetBootstrapServers, c.targetMissEndTopic, new SimpleStringSchema()))

    if ("test".equalsIgnoreCase(c.runMode)) {
      endMissStream.printToErr()
    }
  }

  class MergeFunction extends KeyedProcessFunction[String, (String, String), String] {
    lazy val recordsState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[String, Seq[String], String]("records-state", new DFLogAggregateFunction, implicitly[TypeInformation[Seq[String]]]))
    lazy val beginState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("begin-state", classOf[Boolean]))
    lazy val endState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("end-state", classOf[Boolean]))
    lazy val timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))
    lazy val missBeginStream = new OutputTag[String]("miss-begin-stream")
    lazy val missEndStream = new OutputTag[String]("miss-end-stream")

    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      val line = value._2
      val trimLine = line.trim
      val lowerCaseLine = trimLine.toLowerCase
      L.info("数据到来:{}", line)
      if (lowerCaseLine.contains("recipe start recipe:")) {
        recordsState.add(trimLine)
        beginState.update(true)
        if (!endState.value()) {
          if (timerState.value == 0L) {
            val ts = ctx.timestamp() + 2 * 60 * 1000
            ctx.timerService.registerEventTimeTimer(ts)
            L.info("start中注册:{}", ts)
            timerState.update(ts)
          }
          return
        }
        out.collect(recordsState.get)

        ctx.timerService().deleteEventTimeTimer(timerState.value())

        recordsState.clear()
        beginState.clear()
        endState.clear()
        timerState.clear()

      } else if (lowerCaseLine.contains("recipe end recipe:")) {
        recordsState.add(trimLine)
        endState.update(true)
        if (!beginState.value()) {
          if (timerState.value == 0L) {
            val ts = ctx.timestamp() - 2 * 60 * 1000
            ctx.timerService.registerEventTimeTimer(ts)
            L.info("end中注册:{}", ts)
            timerState.update(ts)
          }
          return
        }
        out.collect(recordsState.get)

        ctx.timerService().deleteEventTimeTimer(timerState.value())

        recordsState.clear()
        beginState.clear()
        endState.clear()
        timerState.clear()
      } else if (beginState.value || endState.value) {
        recordsState.add(trimLine)
      } else {
        // todo: wtf? 正常来讲不可能begin & end标志都为false，但不排除这种奇葩数据
        // todo: 记录日志或者输出到侧输出流
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)
      L.info("定时器触发了 触发时间:{}, 状态变量中时间:{}", timestamp, timerState.value())
      if (timestamp == timerState.value()) {
        ctx.output(if (beginState.value()) missEndStream else missBeginStream, recordsState.get())

        ctx.timerService().deleteEventTimeTimer(timerState.value())
        recordsState.clear()
        beginState.clear()
        endState.clear()
        timerState.clear()
      }
    }
  }

  class DFLogAggregateFunction extends AggregateFunction[String, Seq[String], String] {

    override def createAccumulator(): Seq[String] = Seq[String]()

    override def add(in: String, acc: Seq[String]): Seq[String] = acc :+ in

    override def getResult(acc: Seq[String]): String = acc.mkString(" ")

    override def merge(acc1: Seq[String], acc2: Seq[String]): Seq[String] = acc1 ++ acc2
  }


  case class DeviceLog(host: String, path: String, message: String)

}
