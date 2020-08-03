package com.aikosolar.bigdata

import java.util.Properties

import com.aikosolar.app.conf.HalmHandleConfig
import com.aikosolar.bigdata.bean.DFlogFull
import com.aikosolar.bigdata.flink.common.utils.Dates
import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  *
  * Halm日志数据预处理器
  *
  * input: kafka
  *
  * output: hbase
  *
  * 逻辑: 根据业务逻辑将数据数据处理以后写入hbase中,
  *
  * 运行参数:
  *
  * flink run -m yarn-cluster \
  * -p 3 \
  * -ys 2 \
  * -yjm 1024 \
  * -ytm 2048 \
  * -ynm HalmFull \
  * --class com.aikosolar.bigdata.HalmFullJob  /root/halm/HalmHandle-1.1.0.jar \
  * --job-name=halmfull \
  * --topic=data-collection-halm-yw  \
  * --bootstrap.servers=172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092 \
  * --group.id=halm_full_processyw_717 \
  * --hbase.table=ods:ods_f_eqp_ct_halmyw \
  * --checkpointDataUri=hdfs://172.16.98.85:8020/flink-checkpoint \
  * --reset.strategy=latest \
  * --hbase.writeBufferSize=52428800
  *
  * @author carlc
  */
object DFlogCtJob extends FLinkRunner[HalmHandleConfig] {
  /**
    * 业务方法[不需自己调用env.execute()]
    */
  override def run(env: StreamExecutionEnvironment, c: HalmHandleConfig): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", c.bootstrapServers)
    props.setProperty("group.id", c.groupId)

    val source = new FlinkKafkaConsumer010[String](c.topic, new SimpleStringSchema, props)

    c.resetStrategy.toLowerCase() match {
      case "earliest" => source.setStartFromEarliest()
      case "latest" => source.setStartFromLatest()
      case "groupoffsets" => source.setStartFromGroupOffsets()
      case "none" =>
    }

    val stream = env.addSource(source).map(json => DFlogFull(json))
    stream.keyBy(line => (line.EqpID, line.TubeID))
    val streamct = stream.assignAscendingTimestamps(data => Dates.string2Long(data.updatetime, Dates.fmt2))
      .keyBy(line => (line.EqpID, line.TubeID))
      .countWindow(2, 1)
      .reduce(new ReduceFunction[DFlogFull] {
        override def reduce(t1: DFlogFull, t2: DFlogFull): DFlogFull = {
          DFlogFull(t1.EqpID,
            t1.TubeID,
            t1.updatetime,
            t1.Tube2Text1,
            t1.RunCount,
            t1.Memory_All_Text2,
            t1.Memory_All_Text3,
            t1.Memory_All_Text4,
            t1.E10State,
            t1.Gas_N2_POCl3_VolumeAct: String,
            t1.Gas_POClBubb_TempAct,
            t1.Gas_POClBubb_Level,
            t1.DataVar_All_RunTime: String,
            t1.Vacuum_Door_Pressure: String,
            t1.DataVar_All_Recipe,
            t1.Boat_id,
            t2.updatetime)
        }
      })





    // stream.addSink(new HBaseSink[HalmFull](Builder.me().setDurability(c.setDurability).async(c.async).writeBufferSize(c.writeBufferSize.toInt).build(), c.tablename, new UserMutationConverter, HBaseOperation.INSERT))

  }
}





