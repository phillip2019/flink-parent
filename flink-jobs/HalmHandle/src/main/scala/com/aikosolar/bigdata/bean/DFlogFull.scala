package com.aikosolar.bigdata.bean

import com.aikosolar.bigdata.flink.common.utils.Tool
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.StringUtils

case class DFlogFull(
                      val EqpID: String,
                      val TubeID: String,
                      val updatetime: String,
                      val Tube2Text1: String,
                      val RunCount: String,
                      val Memory_All_Text2: String,
                      val Memory_All_Text3: String,
                      val Memory_All_Text4: String,
                      val E10State: String,
                      val Gas_N2_POCl3_VolumeAct: String,
                      val Gas_POClBubb_TempAct: String,
                      val Gas_POClBubb_Level: String,
                      val DataVar_All_RunTime: String,
                      val Vacuum_Door_Pressure: String,
                      val DataVar_All_Recipe: String,
                      val Boat_id: String,
                      var end_time: String
                    )

object DFlogFull {
  def apply(json: String): DFlogFull = {
    if (StringUtils.isNotEmpty(json)) {
      val js = JSON.parseObject(json)
      val data = js.getJSONObject("data")
      if (data != null) {
        val tool_str = new Tool()
        val eqpId: String = tool_str.isEmpty(data.getOrDefault("EqpID", ""))
        val TubeID: String = tool_str.isEmpty(data.getOrDefault("TubeID", ""))
        val eqplength = eqpId.length
        var boatid = ""
        if (StringUtils.isNotBlank(eqpId) && StringUtils.isNotBlank(TubeID)) {
          val Boat = eqpId.substring(eqplength - 4)
          for (i <- 1 until (13)) {
            val BoatNumber = "Boat" + i
            val keyTube = BoatNumber + "@Tube%String"
            val boatTube = tool_str.isEmpty(data.getOrDefault(keyTube, ""))
            if (Boat.equals(boatTube)) {
              val keyboat = BoatNumber + "@BoatID%String"
              boatid = tool_str.isEmpty(data.getOrDefault(keyboat, ""))
            }
          }
        }

        val updatetime = tool_str.isEmpty(data.getOrDefault("updatetime", ""))
        val Tube2Text1 = tool_str.isEmpty(data.getOrDefault("Tube2Text1", ""))
        val RunCount = tool_str.isEmpty(data.getOrDefault("RunCount", ""))
        val Memory_All_Text2 = tool_str.isEmpty(data.getOrDefault("Tube2@Memory@All@Text2%String", ""))
        val Memory_All_Text3 = tool_str.isEmpty(data.getOrDefault("Tube2@Memory@All@Text3%String", ""))
        val Memory_All_Text4 = tool_str.isEmpty(data.getOrDefault("Tube2@Memory@All@Text4%String", ""))
        val E10State = tool_str.isEmpty(data.getOrDefault("Tube2@E10State%Integer", ""))
        val Gas_N2_POCl3_VolumeAct = tool_str.isEmpty(data.getOrDefault("Tube2@Gas@N2_POCl3@VolumeAct%Double", ""))
        val Gas_POClBubb_TempAct = tool_str.isEmpty(data.getOrDefault("Tube2@Gas@POClBubb@TempAct%Float", ""))
        val Gas_POClBubb_Level = tool_str.isEmpty(data.getOrDefault("Tube2@Gas@POClBubb@Level%Float", ""))
        val DataVar_All_RunTime = tool_str.isEmpty(data.getOrDefault("Tube2@DataVar@All@RunTime%Double", ""))
        val Vacuum_Door_Pressure = tool_str.isEmpty(data.getOrDefault("Tube2@Vacuum@Door@Pressure%Float", ""))
        val DataVar_All_Recipe = tool_str.isEmpty(data.getOrDefault("Tube2@DataVar@All@Recipe%String", ""))
        val end_time = ""
        return DFlogFull(eqpId, TubeID, updatetime, Tube2Text1, RunCount, Memory_All_Text2, Memory_All_Text3, Memory_All_Text4,
          E10State, Gas_N2_POCl3_VolumeAct, Gas_POClBubb_TempAct, Gas_POClBubb_Level, DataVar_All_RunTime, Vacuum_Door_Pressure
          , DataVar_All_Recipe, boatid, end_time)
      }
    }
    null
  }


}






