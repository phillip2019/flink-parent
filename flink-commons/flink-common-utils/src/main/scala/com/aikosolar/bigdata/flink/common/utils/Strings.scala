package com.aikosolar.bigdata.flink.common.utils

import java.util.regex.Pattern

/**
  * @author carlc
  */
object Strings {
  val EQPID_REGEX: Pattern = Pattern.compile("[A-Z]\\d-[A-Z]+\\d{1,2}")

  /**
    * 驼峰转下滑
    */
  def humpToUnderLine(str: String): String = {
    var result = ""
    for (char <- str) {
      if (char.isUpper) result += "_" + char else result += char
    }
    if (result.startsWith("_")) result.substring(1) else result
  }

  /**
    * 驼峰转下滑(全小写)
    */
  def humpToUnderLowerString(str: String): String = {
    humpToUnderLine(str).toLowerCase()
  }

  //判断对象是否为空或者空字符串
  def getNotnull(obj: Object): String = {
    val result = if (obj == null || "".equals(obj.toString)) "" else obj.toString
    result
  }

  /**
    * 判断eqpid是否为合法格式
    */
  def isValidEqpId(obj: Object): Boolean = {
    if (obj == null) false else EQPID_REGEX.matcher(obj.toString.trim.toUpperCase()).matches()
  }

  def main(args: Array[String]): Unit = {
    println(isValidEqpId("Z2-DFH05"))
    println(isValidEqpId("2000-BHFU03"))
    println(isValidEqpId(" Z2-HFU06"))
    println(isValidEqpId("Z2-05"))
    println(isValidEqpId("Z205"))
  }
}
