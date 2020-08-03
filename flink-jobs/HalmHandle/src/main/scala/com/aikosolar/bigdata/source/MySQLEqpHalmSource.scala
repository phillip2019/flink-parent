package com.aikosolar.bigdata.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.aikosolar.bigdata.bean.EqpHalm
import com.aikosolar.bigdata.conf.HalmAlarmConfig
import com.aikosolar.bigdata.flink.common.utils.IOUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source._

class MySQLEqpHalmSource(sleepMs: Long) extends RichSourceFunction[EqpHalm] with CheckpointedFunction {

  var running: Boolean = _
  var connection: Connection = _
  var pstmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    val conf = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[HalmAlarmConfig]
    running = true

    Class.forName("com.mysql.jdbc.Driver")
    connection = DriverManager.getConnection(conf.mysqlUrl, conf.mysqlUsername, conf.mysqlPassword)
    pstmt = connection.prepareStatement(conf.mysqlSQL)
  }


  def fetchEqpids(): Seq[String] = {
    val rs: ResultSet = pstmt.executeQuery()
    var seq: Seq[String] = Seq[String]()
    while (rs.next()) {
      seq :+= rs.getString("eqp_id")
    }
    seq
  }

  override def run(ctx: SourceFunction.SourceContext[EqpHalm]): Unit = {
    // 初始时间
    val initTime = System.currentTimeMillis()
    // 当前时间
    var current = initTime
    var checkTime = initTime
    var ids: Seq[String] = fetchEqpids()

    while (running) {
      ids.map(EqpHalm(_, current)).foreach(ctx.collect(_))
      current += sleepMs
      Thread.sleep(sleepMs)
      // 时间差值(秒钟)大于12小时，则从数据库中拉取一次
      // 如果从数据库拉取失败，则继续使用上一次的结果集
      if ((System.currentTimeMillis() - checkTime) / 1000 >= 12 * 60 * 60) {
        try {
          val newIds: Seq[String] = fetchEqpids()
          if (!newIds.isEmpty) ids = newIds
        } catch {
          case e: Exception => {
            // no-op
          }
        } finally {
          checkTime = System.currentTimeMillis()
        }
      }
    }
  }

  override def cancel(): Unit = {
    running = false
    IOUtils.closeQuietly(pstmt)
    IOUtils.closeQuietly(connection)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    // todo
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    // todo
  }
}