package com.aikosolar.bigdata.flink.table

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row

/**
  * 读取csv文件并写入mysql
  *
  * @author carlc
  */
object CsvToMySQLDemo {

  def main(args: Array[String]): Unit = {
    // >>>>>>> 套路 >>>>>>>
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    // <<<<<<< 套路 <<<<<<<

    // -----------下面2个是等价的，2选1----------------
    // useApi2CreateCsvBT(tEnv)
    useDDL2CreateCsvBT(tEnv)
    // -----------上面2个是等价的，2选1----------------

    useDDL2CreateMySQLBT(tEnv)


    // 控制台打印, table需转换为DataStream(这里转AppendStream,根据语句不同，选择不同类型)再打印
    val querySQL =
      """
        |SELECT * FROM csv_bt
      """.stripMargin
    val result = tEnv.sqlQuery(querySQL)
    result.toAppendStream[Row].print("控制台打印")

    // 连接CSV数据写入MySQL
    val csv2MySQL =
      """
        |INSERT INTO mysql_bt
        |SELECT * FROM csv_bt
      """.stripMargin
    tEnv.sqlUpdate(csv2MySQL)

    env.execute("FLinkTableCsv")
  }

  /**
    * 使用Table API创建csv_bt表
    */
  def useApi2CreateCsvBT(tEnv:StreamTableEnvironment): Unit ={
    // table api 方式(注意:1.9 版本和 1.11 版本Api的差异，部分api后续将被废弃)
    tEnv.connect(new FileSystem().path("/home/carlc/工作/aikosolar-flink-parent/flink-jobs/flink-table-study/src/main/resources/bt.csv"))
      .withFormat(new OldCsv()
//        .ignoreFirstLine()              // api版本支持过滤第一个条数据，但在ddl里面好像又不能，所以我把原数据中第一行删除了
        .field("EqpID",Types.STRING)
        .field("ModuleName",Types.STRING)
        .field("E10State",Types.STRING)
        .field("StatusCode",Types.STRING)
        .field("ProcessStatus",Types.STRING)
        .field("PutTime",Types.STRING)
        .quoteCharacter('"')
      )
      .withSchema(new Schema()
        .field("EqpID",Types.STRING)
        .field("ModuleName",Types.STRING)
        .field("E10State",Types.STRING)
        .field("StatusCode",Types.STRING)
        .field("ProcessStatus",Types.STRING)
        .field("PutTime",Types.STRING)
      )
      .registerTableSource("csv_bt")
  }

  /**
    * 使用DDL创建csv_bt表
    */
  def useDDL2CreateCsvBT(tEnv:StreamTableEnvironment): Unit = {
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

    tEnv.sqlUpdate(ddl)
  }


  /**
    * 使用DDL创建mysql_bt表
    */
  def useDDL2CreateMySQLBT(tEnv:StreamTableEnvironment): Unit = {

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
        | 'connector.url' = 'jdbc:mysql://mysql:3306/maple', -- required: JDBC DB url
        | 'connector.table' = 't_bt',
        | 'connector.driver' = 'com.mysql.jdbc.Driver',
        | 'connector.username' = 'root',
        | 'connector.password' = 'root'
        |)
      """.stripMargin

    tEnv.sqlUpdate(ddl)
  }
}
