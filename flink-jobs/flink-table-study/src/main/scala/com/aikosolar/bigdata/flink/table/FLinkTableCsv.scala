package com.aikosolar.bigdata.flink.table

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row


/**
  * FLink Table/SQL API 学习
  *
  * @author carlc
  */
object FLinkTableCsv {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    // table api 方式
    tEnv.connect(new FileSystem().path("/home/carlc/工作/aikosolar-flink-parent/flink-jobs/flink-table-study/src/main/resources/bt.csv"))
      .withFormat(new OldCsv().ignoreFirstLine()
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

    // ddl 方式
    val mysqlCreateTable =
      """
        |CREATE TABLE mysql_csv (
        |    EqpID VARCHAR,
        |    ModuleName VARCHAR,
        |    E10State VARCHAR,
        |    StatusCode VARCHAR,
        |    ProcessStatus VARCHAR,
        |    PutTime VARCHAR
        |)
        |WITH (
        |    'connector.type' = 'jdbc',
        |    'connector.url' = 'jdbc:mysql://mysql:3306/maple', -- required: JDBC DB url
        |    'connector.table' = 't_bt',
        |    'connector.driver' = 'com.mysql.jdbc.Driver',
        |    'connector.username' = 'root',
        |    'connector.password' = 'root'
        |)
      """.stripMargin

    tEnv.sqlUpdate(mysqlCreateTable)

    val querySQL =
      """
        |SELECT * FROM csv_bt
      """.stripMargin
    val result = tEnv.sqlQuery(querySQL)
    result.toAppendStream[Row].print("控制台打印")

    val insertSQL =
      """
        |INSERT INTO mysql_csv
        |SELECT * FROM csv_bt
      """.stripMargin

    tEnv.sqlUpdate(insertSQL)
    env.execute("FLinkSqlDemo")
  }
}
