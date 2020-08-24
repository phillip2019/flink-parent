package com.aikosolar.bigdata.flink.common.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.Locale

import com.aikosolar.bigdata.flink.common.enums.Sites

/**
  * 日期工具类
  *
  * @author carlc
  */
object Dates {
  lazy val fmt1: DateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss", Locale.CHINA)
  lazy val fmt2: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA)
  lazy val fmt3: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss", Locale.CHINA)
  lazy val fmt4: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm", Locale.CHINA)
  lazy val fmt5: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.CHINA)
  lazy val fmt6: DateTimeFormatter = DateTimeFormatter.ofPattern("yyMMddHHmm", Locale.CHINA)

  lazy val zone_cn = ZoneOffset.of("+8")

  def long2String(ts: Long, fmt: DateTimeFormatter): String = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), zone_cn).format(fmt)

  def string2Long(date: String, fmt: DateTimeFormatter): Long = LocalDateTime.parse(date, fmt).toInstant(zone_cn).toEpochMilli()

  def now(fmt: DateTimeFormatter): String = LocalDateTime.now(zone_cn).format(fmt)

  /**
    * 时间转换为班次
    * 浙江基地:8:00-20:00
    * 天津基地:8:00-20:00
    * 广东基地:7:30-19:30
    */
  def toShift(date: String, fmt: DateTimeFormatter, site: String): String = {
    val s = Sites.toSite(site)
    if (s != null) s.toShift(LocalDateTime.parse(date, fmt)) else null
  }

  /**
    * 日期转换为班次字符串
    */
  def toShiftChar(date: String, fmt: DateTimeFormatter, site: String): String = {
    val x = toShift(date, fmt, site)
    if (x == null) x else x.substring(x.length - 1)
  }

  /**
    * 日期转换为班次字符串
    */
  def toSwitchShiftTime(date: String, fmt: DateTimeFormatter, site: String): Long = {
    val s = Sites.toSite(site)
    if (s != null) {
      val x = s.toSwitchShiftTime(LocalDateTime.parse(date, fmt))
      if (x != null) {
        string2Long(x, fmt2)
      } else {
        -1L
      }
    } else -1L
  }
}
