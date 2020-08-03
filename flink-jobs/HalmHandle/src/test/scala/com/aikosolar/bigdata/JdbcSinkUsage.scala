package com.aikosolar.bigdata

import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicInteger

import com.aikosolar.bigdata.flink.connectors.jdbc.JdbcSink
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions
import com.aikosolar.bigdata.flink.connectors.jdbc.writter.JdbcWriter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object JdbcSinkUsage {

  /**
    * 仅演示如何使用JdbcSink,具体请根据实际情况调整
    */
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val rawStream: DataStream[String] = env.addSource(new RichSourceFunction[String] {

      var running: Boolean = _

      override def open(parameters: Configuration): Unit = {
        running = true
      }

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

        val counter = new AtomicInteger(1)
        while (running) {
          ctx.collect(counter.getAndIncrement().toString)
          Thread.sleep(10 * 1000)
        }
      }

      override def cancel(): Unit = {
        running = false
      }

    })

    val userStream: DataStream[User] = rawStream
      // 先map成3元组
      .map(x => (User(x.toInt, s"name-$x"), Profile(x.toInt, s"email-$x", s"address-$x"), UserProfile(x.toInt, x.toInt, x.toInt)))
      // 分流:(user为主输),Profile/UserProfile为侧输出流
      .process(new ProcessFunction[(User, Profile, UserProfile), User] {
      lazy val profileStream = new OutputTag[Profile]("profileStream")
      lazy val userProfileStream = new OutputTag[UserProfile]("userProfileStream")

      override def processElement(value: (User, Profile, UserProfile), ctx: ProcessFunction[(User, Profile, UserProfile), User]#Context, out: Collector[User]): Unit = {
        // 我们认为User为主输出流, Profile/UserProfile 为侧输出流
        out.collect(value._1)

        ctx.output(profileStream, value._2)
        ctx.output(userProfileStream, value._3)
      }
    })


    // 获取侧输出流(Profile)
    val profileStream: DataStream[Profile] = userStream.getSideOutput(new OutputTag[Profile]("profileStream"))

    // 获取侧输出流(UserProfile)
    val userProfileStream: DataStream[UserProfile] = userStream.getSideOutput(new OutputTag[UserProfile]("userProfileStream"))

    userStream.print("用户表")
    profileStream.print("简历表")
    userProfileStream.print("关联表")

    // 创建输出配置对象
    val conf = new JdbcConnectionOptions.Builder()
      .withDriverName("com.mysql.jdbc.Driver")
      .withUrl("jdbc:mysql://mysql:3306/maple?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
      .withUsername("root")
      .withPassword("root")
      .build()

    // 写入User表
    userStream.addSink(new JdbcSink[User](conf, "insert into t_user(id,name) values(?,?)", new UserJdbcWriter))

    // 写入Profile表
    profileStream.addSink(new JdbcSink[Profile](conf, "insert into t_profile(id,email,address) values(?,?,?)", new ProfileJdbcWriter))

    // 写入UserProfile表
    userProfileStream.addSink(new JdbcSink[UserProfile](conf, "insert into t_user_profile(id,user_id,profile_id) values(?,?,?)", new UserProfileJdbcWriter))

    env.execute("JdbcSinkUsageJob")
  }

  case class User(id: Int, name: String)

  case class Profile(id: Int, email: String, address: String)

  case class UserProfile(id: Int, user_id: Int, profile_id: Int)

  class UserJdbcWriter extends JdbcWriter[User] {
    override def accept(stmt: PreparedStatement, data: User): Unit = {
      stmt.setInt(1, data.id)
      stmt.setString(2, data.name)
    }
  }

  class ProfileJdbcWriter extends JdbcWriter[Profile] {
    override def accept(stmt: PreparedStatement, data: Profile): Unit = {
      stmt.setInt(1, data.id)
      stmt.setString(2, data.email)
      stmt.setString(3, data.address)
    }
  }

  class UserProfileJdbcWriter extends JdbcWriter[UserProfile] {
    override def accept(stmt: PreparedStatement, data: UserProfile): Unit = {
      stmt.setInt(1, data.id)
      stmt.setInt(2, data.user_id)
      stmt.setInt(3, data.profile_id)
    }
  }

}
