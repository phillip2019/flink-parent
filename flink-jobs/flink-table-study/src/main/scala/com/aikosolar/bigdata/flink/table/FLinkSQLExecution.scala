package com.aikosolar.bigdata.flink.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

import scala.collection.JavaConversions._

/**
  * 目标: FLinkSQL方式执行任务(当然可以用官方sql-client),我该怎么描述??????????
  * 最好的方式: 前端UI提交SQL -> DB -> 运行该程序(传入相关ID) -> 程序获取对应配置/sql -> 运行,但现在没有UI
  *
  * @author carlc
  */
object FLinkSQLExecution {

  def main(args: Array[String]): Unit = {
    // >>>>>>> 套路 >>>>>>>
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // <<<<<<< 套路 <<<<<<<

    // todo 如果用参数传进来的话很蛋疼,得想个办法简化...
    // ===>  List<Script> == // query from db
    val manager = ScriptManager.me()
      .addScript(new FLinkSQLScript(createCsvBTDDL, "update", 2))
      .addScript(new FLinkSQLScript(createMySQLBTDDL, "update", 1))
      .addScript(new FLinkSQLScript(createCSV2MySQLDDL, "update", 3))


    // 设置tableEnv的配置参数
    for (s <- manager.getSettings) {
      if (s.isInstanceOf[FlinkTableEnvSetting]) {
        if ("string".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setString(s.key(), s.value().toString)
        } else if ("boolean".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setBoolean(s.key(), s.value().toString.toBoolean)
        } else if ("int".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setInteger(s.key(), s.value().toString.toInt)
        } else if ("long".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setLong(s.key(), s.value().toString.toLong)
        } else if ("float".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setFloat(s.key(), s.value().toString.toFloat)
        } else if ("double(".equalsIgnoreCase(s.valueType())) {
          tEnv.getConfig.getConfiguration.setDouble(s.key(), s.value().toString.toDouble)
        }
      }
    }

    // 执行SQL
    for (s <- manager.getScripts) {
      if (s.isInstanceOf[FLinkSQLScript]) {
        if ("update".equals(s.asInstanceOf[FLinkSQLScript].getSqlType)) {
          println(s"执行语句:${s.getScript}")
          tEnv.sqlUpdate(s.getScript)
        }
      }
    }

    env.execute("FLinkTableCsv")
  }

  /**
    * DDL方式创建csv_bt表
    */
  def createCsvBTDDL(): String = {
    // 1.9 版本的csv写起来感觉很恶心
    val ddl =
      """
        |CREATE TABLE csv_bt (
        | EqpID VARCHAR,
        | ModuleName VARCHAR,
        | E10State VARCHAR,
        | StatusCode VARCHAR,
        | ProcessStatus VARCHAR,
        | PutTime VARCHAR
        |)
        |WITH (
        | 'connector.type' = 'filesystem',
        | 'connector.path' = 'file:///home/carlc/工作/aikosolar-flink-parent/flink-jobs/flink-table-study/src/main/resources/bt.csv',
        | 'format.type' = 'csv',
        | 'format.fields.0.name' = 'EqpID',
        | 'format.fields.0.type' = 'VARCHAR',
        | 'format.fields.1.name' = 'ModuleName',
        | 'format.fields.1.type' = 'VARCHAR',
        | 'format.fields.2.name' = 'E10State',
        | 'format.fields.2.type' = 'VARCHAR',
        | 'format.fields.3.name' = 'StatusCode',
        | 'format.fields.3.type' = 'VARCHAR',
        | 'format.fields.4.name' = 'ProcessStatus',
        | 'format.fields.4.type' = 'VARCHAR',
        | 'format.fields.5.name' = 'PutTime',
        | 'format.fields.5.type' = 'VARCHAR',
        | 'format.quote-character' = '"'  -- 这里是因为数据本身各种带了引号所以这里指定并去除
        |)
      """.stripMargin
    ddl
  }


  /**
    * DDL方式创建mysql_bt表
    */
  def createMySQLBTDDL(): String = {
    val ddl =
      """
        |CREATE TABLE mysql_bt (
        | EqpID VARCHAR,
        | ModuleName VARCHAR,
        | E10State VARCHAR,
        | StatusCode VARCHAR,
        | ProcessStatus VARCHAR,
        | PutTime VARCHAR
        |)
        |WITH (
        | 'connector.type' = 'jdbc',
        | 'connector.url' = 'jdbc:mysql://mysql:3306/maple',
        | 'connector.table' = 't_bt',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = 'root'
        |)
      """.stripMargin
    ddl
  }

  /**
    * DDL方式创建mysql_bt表
    */
  def createCSV2MySQLDDL(): String = {
    """
      |INSERT INTO mysql_bt
      |SELECT * FROM csv_bt
    """.stripMargin
  }

}
