package com.aikosolar.bigdata.flink.common.utils

/**
  * @author carlc
  */
object Strings {

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
}
