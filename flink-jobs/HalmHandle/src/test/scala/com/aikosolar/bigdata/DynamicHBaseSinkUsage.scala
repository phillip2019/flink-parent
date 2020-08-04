package com.aikosolar.bigdata

import java.util.concurrent.atomic.AtomicInteger
import java.util.{HashMap, Map}

import com.aikosolar.bigdata.flink.connectors.hbase.DynamicHBaseSink
import com.aikosolar.bigdata.flink.connectors.hbase.constants.Constants
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

/**
  * 仅演示如何使用DynamicHBaseSink,具体请根据实际情况调整
  * 1. 创建HBase表(ods:carlc_test1,ods:carlc_test2) 列簇为c1
  * 2. 添加hbase-site.xml或者指定配置项
  * 3. 运行该程序
  *
  * @author carlc
  */
object DynamicHBaseSinkUsage {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 这里我模拟数据(发送消息1，2，3，4，5...)
    val rawStream: DataStream[Int] = env.addSource(new RichSourceFunction[Int] {
      var running: Boolean = _

      override def open(parameters: Configuration): Unit = {
        running = true
      }

      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        val counter = new AtomicInteger(1)
        while (running) {
          ctx.collect(counter.getAndIncrement())
          Thread.sleep(10 * 1000)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    })

    // map ( 偶数: t1 , 奇数: t2)
    val dataStream: DataStream[Map[String, AnyRef]] = rawStream.map(x => {
      val result: Map[String, AnyRef] = new HashMap[String, AnyRef]()
      result.put(Constants.ROW_KEY, x.toString)
      if (x % 2 == 0) {
        result.put(Constants.topic, "t1")
        result.put("name1", s"name1-$x")
        result.put("name2", s"name2-$x")
      } else {
        result.put(Constants.topic, "t2")
        result.put("address1", s"address1-$x")
        result.put("address2", s"address2-$x")
      }
      result
    })

    // 表映射关系(t1 -> ods:carlc_test1,t2 -> ods:carlc_test2)
    val tableMapping: Map[String, String] = new HashMap[String, String]()
    tableMapping.put("t1", "ods:carlc_test1")
    tableMapping.put("t2", "ods:carlc_test2")

    // 因默认的family为cf
    // 但是我在创建表时，指定为c1
    // default : c1
    val familyMapping: Map[String, String] = new HashMap[String, String]()
    familyMapping.put(Constants.DEFAULT_FAMILY_KEY, "c1")

    // 写入HBase
    dataStream.addSink(new DynamicHBaseSink(Builder.me().build(), tableMapping, familyMapping))

    env.execute("DynamicHBaseSinkUsage")
  }
}
