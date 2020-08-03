package com.aikosolar.bigdata.bean

import com.aikosolar.bigdata.flink.common.utils.Dates
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils

/**
  *
  * @author carlc
  */
case class EqpHalm(eqpId: String, ts: Long)

case class EqpHalmWithStatus(eqpId: String, status: Int, ts: Long)

object EqpHalm {
  def apply(json: String): EqpHalm = {
    if (StringUtils.isNotEmpty(json)) {
      val js = JSON.parseObject(json)
      val data = js.getJSONObject("data")
      if (data != null) {
        val eqpId: String = data.getString("eqp_id")
        val testDate: String = data.getString("TestDate")
        val testTime: String = data.getString("TestTime")
        if (StringUtils.isNotBlank(eqpId) && StringUtils.isNotBlank(testDate) && StringUtils.isNotBlank(testTime)) {
          return new EqpHalm(eqpId.trim, Dates.string2Long(testDate.trim + " " + testTime.trim, Dates.fmt1))
        }
      }
    }
    null
  }
}