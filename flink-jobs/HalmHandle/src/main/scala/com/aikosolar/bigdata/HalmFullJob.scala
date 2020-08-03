package com.aikosolar.bigdata
import java.util.Properties

import com.aikosolar.app.conf.HalmHandleConfig
import com.aikosolar.bigdata.bean.HalmFull
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig.Builder
import com.aikosolar.bigdata.flink.connectors.hbase.{HBaseOperation, HBaseSink}
import com.aikosolar.bigdata.flink.job.base.FLinkRunner
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.apache.hadoop.hbase.util.Bytes

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
 *flink run -m yarn-cluster \
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
object HalmFullJob extends FLinkRunner[HalmHandleConfig]{
  /**
   * 业务方法[不需自己调用env.execute()]
   */
  override def run(env: StreamExecutionEnvironment, c: HalmHandleConfig): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", c.bootstrapServers)
    props.setProperty("group.id", c.groupId)

    val source=new FlinkKafkaConsumer010[String](c.topic, new SimpleStringSchema, props)

    c.resetStrategy.toLowerCase() match {
      case "earliest" => source.setStartFromEarliest()
      case "latest" => source.setStartFromLatest()
      case "groupoffsets" => source.setStartFromGroupOffsets()
      case "none" =>
    }

    val stream = env.addSource(source)
      .map(json=>HalmFull(json))
    stream.addSink(new HBaseSink[HalmFull](Builder.me().setDurability(c.setDurability).async(c.async).writeBufferSize(c.writeBufferSize.toInt).build(), c.tablename, new HalmMutationConverter, HBaseOperation.INSERT))

  }
  class HalmMutationConverter extends HBaseMutationConverter[HalmFull] {
    override def insert(data: HalmFull): Put = {
      val put: Put = new Put(Bytes.toBytes(data.rowkey))

       //写全量表数据
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("site"), Bytes.toBytes( data.site))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("begin_date"), Bytes.toBytes( data.begin_date))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("shift"), Bytes.toBytes(data.shift))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("uniqueid"), Bytes.toBytes( data.uniqueid))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("batchid"), Bytes.toBytes( data.batchid))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("title"), Bytes.toBytes( data.title))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("celltyp"), Bytes.toBytes( data.celltyp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("begin_time"), Bytes.toBytes( data.begin_time))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("comments"), Bytes.toBytes( data.comments))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("operator"), Bytes.toBytes( data.operator))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("classification"), Bytes.toBytes( data.classification))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("bin"), Bytes.toBytes( data.bin))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("uoc"), Bytes.toBytes( data.uoc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("isc"), Bytes.toBytes( data.isc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("ff"), Bytes.toBytes( data.ff))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("eta"), Bytes.toBytes( data.eta))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_uoc"), Bytes.toBytes( data.m3_uoc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_isc"), Bytes.toBytes( data.m3_isc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_ff"), Bytes.toBytes( data.m3_ff))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_eta"), Bytes.toBytes( data.m3_eta))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("jsc"), Bytes.toBytes( data.jsc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("iscuncorr"), Bytes.toBytes( data.iscuncorr))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("uocuncorr"), Bytes.toBytes( data.uocuncorr))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("impp"), Bytes.toBytes( data.impp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("jmpp"), Bytes.toBytes( data.jmpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("umpp"), Bytes.toBytes( data.umpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pmpp"), Bytes.toBytes( data.pmpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("ff_abs"), Bytes.toBytes( data.ff_abs))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("ivld1"), Bytes.toBytes( data.ivld1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("jvld1"), Bytes.toBytes( data.jvld1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("uvld1"), Bytes.toBytes( data.uvld1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pvld1"), Bytes.toBytes( data.pvld1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("ivld2"), Bytes.toBytes( data.ivld2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("jvld2"), Bytes.toBytes( data.jvld2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("uvld2"), Bytes.toBytes( data.uvld2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pvld2"), Bytes.toBytes( data.pvld2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("measuretimelf"), Bytes.toBytes( data.measuretimelf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("crvuminlf"), Bytes.toBytes( data.crvuminlf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("crvumaxlf"), Bytes.toBytes( data.crvumaxlf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("monicellspectralmismatch"), Bytes.toBytes( data.monicellspectralmismatch))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("tmonilf"), Bytes.toBytes( data.tmonilf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rser"), Bytes.toBytes( data.rser))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("sser"), Bytes.toBytes( data.sser))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rshunt"), Bytes.toBytes( data.rshunt))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("sshunt"), Bytes.toBytes( data.sshunt))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("tcell"), Bytes.toBytes( data.tcell))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("cellparamtki"), Bytes.toBytes( data.cellparamtki))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("cellparamtku"), Bytes.toBytes( data.cellparamtku))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("cellparamarea"), Bytes.toBytes( data.cellparamarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("cellparamtyp"), Bytes.toBytes( data.cellparamtyp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("tenv"), Bytes.toBytes( data.tenv))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("tmonicell"), Bytes.toBytes( data.tmonicell))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("insol"), Bytes.toBytes( data.insol))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("insolmpp"), Bytes.toBytes( data.insolmpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("correctedtoinsol"), Bytes.toBytes( data.correctedtoinsol))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("cellidstr"), Bytes.toBytes( data.cellidstr))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("irevmax"), Bytes.toBytes( data.irevmax))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rserlf"), Bytes.toBytes( data.rserlf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rshuntlf"), Bytes.toBytes( data.rshuntlf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("monicellmvtoinsol"), Bytes.toBytes( data.monicellmvtoinsol))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("flashmonicellvoltage"), Bytes.toBytes( data.flashmonicellvoltage))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2class"), Bytes.toBytes( data.el2class))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2contactissue"), Bytes.toBytes( data.el2contactissue))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2crackdefaultcount"), Bytes.toBytes( data.el2crackdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2crystaldefaultarea"), Bytes.toBytes( data.el2crystaldefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2crystaldefaultcount"), Bytes.toBytes( data.el2crystaldefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2crystaldefaultseverity"), Bytes.toBytes( data.el2crystaldefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2darkdefaultarea"), Bytes.toBytes( data.el2darkdefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2darkdefaultcount"), Bytes.toBytes( data.el2darkdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2darkdefaultseverity"), Bytes.toBytes( data.el2darkdefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2darkgrainboundary"), Bytes.toBytes( data.el2darkgrainboundary))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2edgebrick"), Bytes.toBytes( data.el2edgebrick))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingercontinuousdefaultarea"), Bytes.toBytes( data.el2fingercontinuousdefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingercontinuousdefaultcount"), Bytes.toBytes( data.el2fingercontinuousdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingercontinuousdefaultseverity"), Bytes.toBytes( data.el2fingercontinuousdefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingerdefaultarea"), Bytes.toBytes( data.el2fingerdefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingerdefaultcount"), Bytes.toBytes( data.el2fingerdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2fingerdefaultseverity"), Bytes.toBytes( data.el2fingerdefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2firing"), Bytes.toBytes( data.el2firing))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2grippermark"), Bytes.toBytes( data.el2grippermark))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2data"), Bytes.toBytes(data.el2line))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2oxring"), Bytes.toBytes( data.el2oxring))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2scratchdefaultarea"), Bytes.toBytes( data.el2scratchdefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2scratchdefaultcount"), Bytes.toBytes( data.el2scratchdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2scratchdefaultseverity"), Bytes.toBytes( data.el2scratchdefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2spotdefaultarea"), Bytes.toBytes( data.el2spotdefaultarea))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2spotdefaultcount"), Bytes.toBytes( data.el2spotdefaultcount))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2spotdefaultseverity"), Bytes.toBytes( data.el2spotdefaultseverity))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("elbin"), Bytes.toBytes( data.elbin))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("elbincomment"), Bytes.toBytes( data.elbincomment))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("elcamexposuretime"), Bytes.toBytes( data.elcamexposuretime))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("elcamgain"), Bytes.toBytes( data.elcamgain))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2evalrecipe"), Bytes.toBytes( data.el2evalrecipe))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pecon"), Bytes.toBytes( data.pecon))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pectsys"), Bytes.toBytes( data.pectsys))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pecyt"), Bytes.toBytes( data.pecyt))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pepmt"), Bytes.toBytes( data.pepmt))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("pepreviouspinotreadyformeasurement"), Bytes.toBytes( data.pepreviouspinotreadyformeasurement))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("el2timeevaluation"), Bytes.toBytes( data.el2timeevaluation))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_isc"), Bytes.toBytes( data.m2_isc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_uoc"), Bytes.toBytes( data.m2_uoc))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_pmpp"), Bytes.toBytes( data.m2_pmpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_ff"), Bytes.toBytes( data.m2_ff))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_eta"), Bytes.toBytes( data.m2_eta))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_insol"), Bytes.toBytes( data.m2_insol))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_pmpp"), Bytes.toBytes( data.m3_pmpp))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_insol"), Bytes.toBytes( data.m3_insol))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_insol_m1"), Bytes.toBytes( data.m2_insol_m1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m2_insol_m2"), Bytes.toBytes( data.m2_insol_m2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("insol_m1"), Bytes.toBytes( data.insol_m1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("insol_m2"), Bytes.toBytes( data.insol_m2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_insol_m1"), Bytes.toBytes( data.m3_insol_m1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("m3_insol_m2"), Bytes.toBytes( data.m3_insol_m2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("eqp_id"), Bytes.toBytes( data.eqp_id))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("aoi_1_q"), Bytes.toBytes( data.aoi_1_q))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("aoi_1_r"), Bytes.toBytes( data.aoi_1_r))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("aoi_2_q"), Bytes.toBytes( data.aoi_2_q))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("aoi_2_r"), Bytes.toBytes( data.aoi_2_r))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("irev2"), Bytes.toBytes( data.irev2))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("irev1"), Bytes.toBytes( data.irev1))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rshuntdr"), Bytes.toBytes( data.rshuntdr))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rshuntdf"), Bytes.toBytes( data.rshuntdf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rserlfdf"), Bytes.toBytes( data.rserlfdf))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("bin_comment"), Bytes.toBytes( data.bin_comment))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("elmeangray"), Bytes.toBytes( data.elmeangray))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("rshuntdfdr"), Bytes.toBytes(data.rshuntdfdr))
      put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("factory"), Bytes.toBytes(data.factory))

      //dtl表数据
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("batch_id"),Bytes.toBytes(data.batchid))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("test_time"),Bytes.toBytes(data.testtime))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("test_date" ),Bytes.toBytes(data.testdate))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("site" ),Bytes.toBytes(data.site))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("begin_time"),Bytes.toBytes(data.begin_time))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("begin_date"),Bytes.toBytes(data.begin_date))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("shift" ),Bytes.toBytes(data.shift))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("eqp_id" ),Bytes.toBytes(data.eqp_id))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("comments"),Bytes.toBytes(data.comments))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("bin" ),Bytes.toBytes(data.bin))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("bin_comment"),Bytes.toBytes(data.bin_comment))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("uoc" ),Bytes.toBytes(data.uoc))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("isc"),Bytes.toBytes(data.isc))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("ff"),Bytes.toBytes(data.ff))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("eta"),Bytes.toBytes(data.eta))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("m3_eta"),Bytes.toBytes(data.m3_eta))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("irev2"),Bytes.toBytes(data.irev2))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("rser" ),Bytes.toBytes(data.rser))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("rshunt"),Bytes.toBytes(data.rshunt))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("tcell"),Bytes.toBytes(data.tcell))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("tmonicell"),Bytes.toBytes(data.tmonicell))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("elmeangray"),Bytes.toBytes(data.elmeangray))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("aoi_2_r"),Bytes.toBytes(data.aoi_2_r))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("el2fingerdefaultcount"),Bytes.toBytes(data.el2fingerdefaultcount))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("cellparamtki" ),Bytes.toBytes(data.cellparamtki))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("cellparamtku"),Bytes.toBytes(data.cellparamtku))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("insol_m1"),Bytes.toBytes(data.insol_m1))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("m3_insol"),Bytes.toBytes(data.m3_insol))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("bin_type"),Bytes.toBytes(data.bin_type))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("order_type"),Bytes.toBytes(data.order_type))
      put.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("factory"),Bytes.toBytes(data.factory))
      put
    }

    override def delete(data: HalmFull): Delete = {
      null
    }
  }
}



