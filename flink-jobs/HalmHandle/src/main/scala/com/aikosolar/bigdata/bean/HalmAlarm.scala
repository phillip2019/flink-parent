package com.aikosolar.bigdata.bean

case class HalmAlarm(alarm_id: String, factory: String, site: String,
                     eqp_id: String, sub_process: String, begin_time: String,
                     end_time: String, status: Int, cnt: Int,
                     msg: String, create_time: String)