package com.aikosolar.bigdata.flink.job

import java.util
import java.util.Map

import com.aikosolar.bigdata.flink.connectors.hbase.SimpleHBaseTableSink
import com.aikosolar.bigdata.flink.connectors.hbase.utils.RowKeyGenerator
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.job.conf.GpDataLoaderConf
import com.aikosolar.bigdata.flink.job.utils.GPUtils
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.util.Collector
import org.apache.log4j.Logger


/**
  * --job-name=GpDataLoaderJob
  * --time-characteristic=EventTime
  * --bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
  * --topic=t1
  * --group.id=g1
  * --target.bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
  * --target.topic=t2
  * --checkpointDataUri=hdfs://172.16.98.85:8020/flink-checkpoint
  * --gp1.hbase.table=xxx1
  * --gp2.hbase.table=xxx2
  *
  * @author carlc
  */
object GpDataLoaderJob extends FLinkKafkaRunner[GpDataLoaderConf] {

  val logger: Logger = Logger.getLogger(GpDataLoaderJob.getClass)

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run0(env: StreamExecutionEnvironment, c: GpDataLoaderConf, rawKafkaSource: DataStream[String]): Unit = {
    val mapStream: DataStream[Map[String, AnyRef]] = rawKafkaSource
      .map(x => GPUtils.parse(x))
      .filter(_ != null)
      .process(new ProcessFunction[Map[String, AnyRef], Map[String, AnyRef]] {
        lazy val Gp2Stream = new OutputTag[Map[String, AnyRef]]("GP2")

        override def processElement(value: util.Map[String, AnyRef], ctx: ProcessFunction[util.Map[String, AnyRef], util.Map[String, AnyRef]]#Context, out: Collector[util.Map[String, AnyRef]]): Unit = {
          val gpType = value.remove(GPUtils.GP_TYPE_KEY)
          if (GPUtils.GP_TYPE1.equals(gpType)) {
            out.collect(value)
          } else {
            ctx.output(Gp2Stream, value)
          }
        }
      })
    mapStream.print("GP1")
    mapStream.getSideOutput(new OutputTag[Map[String, String]]("GP2")).print("GP2")

    // --------------------------- GP1 ---------------------------
    mapStream.map(x => JSON.toJSONString(x)).addSink(new FlinkKafkaProducer010[String](c.targetBootstrapServers, c.targetTopic, new SimpleStringSchema()))
    mapStream.map(x => {
      x.put("row_key", RowKeyGenerator.gen(x.get("EquipmentID").toString, x.get("Come").toString))
      x
    })
      .addSink(new SimpleHBaseTableSink(Builder.me().build(), c.gp1tableName))

    // --------------------------- GP2 ---------------------------
    mapStream.getSideOutput(new OutputTag[Map[String, AnyRef]]("GP2"))
      .map(x => {
        x.put("row_key", RowKeyGenerator.gen(x.get("EquipmentID").toString, x.get("Come").toString))
        x
      })
      .addSink(new SimpleHBaseTableSink(Builder.me().build(), c.gp2tableName))
  }
}
