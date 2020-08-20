package com.aikosolar.bigdata.bean

import java.text.SimpleDateFormat

import com.aikosolar.bigdata.flink.common.utils.Tool
import com.alibaba.fastjson.JSON

case class HalmFull(
                     var rowkey: String,
                     var site: String,
                     var begin_date: String,
                     var shift: String,
                     var uniqueid: String,
                     var batchid: String,
                     var title: String,
                     var celltyp: String,
                     var begin_time: String,
                     var comments: String,
                     var operator: String,
                     var classification: String,
                     var bin: String,
                     var uoc: String,
                     var isc: String,
                     var ff: String,
                     var eta: String,
                     var m3_uoc: String,
                     var m3_isc: String,
                     var m3_ff: String,
                     var m3_eta: String,
                     var jsc: String,
                     var iscuncorr: String,
                     var uocuncorr: String,
                     var impp: String,
                     var jmpp: String,
                     var umpp: String,
                     var pmpp: String,
                     var ff_abs: String,
                     var ivld1: String,
                     var jvld1: String,
                     var uvld1: String,
                     var pvld1: String,
                     var ivld2: String,
                     var jvld2: String,
                     var uvld2: String,
                     var pvld2: String,
                     var measuretimelf: String,
                     var crvuminlf: String,
                     var crvumaxlf: String,
                     var monicellspectralmismatch: String,
                     var tmonilf: String,
                     var rser: String,
                     var sser: String,
                     var rshunt: String,
                     var sshunt: String,
                     var tcell: String,
                     var cellparamtki: String,
                     var cellparamtku: String,
                     var cellparamarea: String,
                     var cellparamtyp: String,
                     var tenv: String,
                     var tmonicell: String,
                     var insol: String,
                     var insolmpp: String,
                     var correctedtoinsol: String,
                     var cellidstr: String,
                     var irevmax: String,
                     var rserlf: String,
                     var rshuntlf: String,
                     var monicellmvtoinsol: String,
                     var flashmonicellvoltage: String,
                     var el2class: String,
                     var el2contactissue: String,
                     var el2crackdefaultcount: String,
                     var el2crystaldefaultarea: String,
                     var el2crystaldefaultcount: String,
                     var el2crystaldefaultseverity: String,
                     var el2darkdefaultarea: String,
                     var el2darkdefaultcount: String,
                     var el2darkdefaultseverity: String,
                     var el2darkgrainboundary: String,
                     var el2edgebrick: String,
                     var el2fingercontinuousdefaultarea: String,
                     var el2fingercontinuousdefaultcount: String,
                     var el2fingercontinuousdefaultseverity: String,
                     var el2fingerdefaultarea: String,
                     var el2fingerdefaultcount: String,
                     var el2fingerdefaultseverity: String,
                     var el2firing: String,
                     var el2grippermark: String,
                     var el2line: String,
                     var el2oxring: String,
                     var el2scratchdefaultarea: String,
                     var el2scratchdefaultcount: String,
                     var el2scratchdefaultseverity: String,
                     var el2spotdefaultarea: String,
                     var el2spotdefaultcount: String,
                     var el2spotdefaultseverity: String,
                     var elbin: String,
                     var elbincomment: String,
                     var elcamexposuretime: String,
                     var elcamgain: String,
                     var el2evalrecipe: String,
                     var pecon: String,
                     var pectsys: String,
                     var pecyt: String,
                     var pepmt: String,
                     var pepreviouspinotreadyformeasurement: String,
                     var el2timeevaluation: String,
                     var m2_isc: String,
                     var m2_uoc: String,
                     var m2_pmpp: String,
                     var m2_ff: String,
                     var m2_eta: String,
                     var m2_insol: String,
                     var m3_pmpp: String,
                     var m3_insol: String,
                     var m2_insol_m1: String,
                     var m2_insol_m2: String,
                     var insol_m1: String,
                     var insol_m2: String,
                     var m3_insol_m1: String,
                     var m3_insol_m2: String,
                     var eqp_id: String,
                     var aoi_1_q: String,
                     var aoi_1_r: String,
                     var aoi_2_q: String,
                     var aoi_2_r: String,
                     var irev2: String,
                     var irev1: String,
                     var rshuntdr: String,
                     var rshuntdf: String,
                     var rserlfdf: String,
                     var bin_comment: String,
                     var elmeangray: String,
                     var rshuntdfdr: String,
                     var testtime: String,
                     var testdate: String,
                     var order_type: String,
                     var bin_type: String,
                     var factory: String
                   )

object HalmFull {
  def apply(json: String): HalmFull = {
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
    val sdf1 = new SimpleDateFormat("dd-MM-yyyy")
    val tool_str = new Tool()
    val js = JSON.parseObject(json)
    var halm2:HalmFull=null
    if(js ==null || js.size()==0 || js.getJSONObject("data")==null || js.getJSONObject("data").size()==0 ) {
      return  halm2
    }else {
      val time = tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestTime", ""))
      val date = tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestDate", "")).toString
      val date1 = if ("".equals(date.trim)) "01-01-1970" else date
      val time2 = if ("".equals(time.trim)) "00:00:00" else time
      val testtime = sdf2.format(sdf1.parse(date1)) + " " + time2
      val eqp_id = tool_str.isEmpty(js.getJSONObject("data").getOrDefault("eqp_id", ""))
      val time1 = if (time == null || time.length < 5) "" else time.substring(0, 5)
      val rowkey = eqp_id + "|" + testtime
      val hbrowkey = tool_str.md5(rowkey) + "|" + rowkey
      val site = if (eqp_id == null || eqp_id.length < 2) "" else eqp_id.substring(0, 2)
      val Begin_Date = if (testtime == null || testtime.length < 10) "" else testtime.substring(0, 10)
      val shift = tool_str.Date_Shift_Time(site, testtime)
      val comments = tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Comment", "")).toUpperCase()
      val bin = tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN", ""))
      val bin_type = if ("0".equals(bin) || "100".equals(bin)) bin else "OTHER"
      val order_type = comments match {
        case comments if comments.contains("YP") => "YP"
        case comments if comments.contains("PL") => "PL"
        case comments if comments.contains("BY") => "BY"
        case comments if comments.contains("YZ") => "YZ"
        case comments if comments.contains("LY") => "LY"
        case comments if comments.contains("CFX") => "CFX"
        case comments if comments.contains("CC") => "CC"
        case comments if comments.contains("GSQ") => "GSQ"
        case comments if comments.contains("GSH") => "GSH"
        case comments if comments.contains("CID") => "CID"
        case _ => "NORMAL"
      }
      val factory = site match {
        case "G1" => "1"
        case "Z1" => "2"
        case "T1" => "3"
        case site if ("Z2".equals(site) || "Z3".equals(site)) => "4"
        case _ => "Other"
      }
      halm2 = HalmFull(
        hbrowkey,
        site,
        Begin_Date,
        shift,
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("UniqueID", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BatchID", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Title", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellTyp", "")),
        testtime,
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Comment", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Operator", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Classification", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Uoc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Isc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("FF", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Eta", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Uoc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Isc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_FF", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Eta", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Jsc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("IscUncorr", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("UocUncorr", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Impp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Jmpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Umpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Pmpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("FF_Abs", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Ivld1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Jvld1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Uvld1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Pvld1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Ivld2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Jvld2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Uvld2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Pvld2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("MeasureTimeLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CrvUminLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CrvUmaxLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("MonicellSpectralMismatch", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TmoniLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Rser", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Sser", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Rshunt", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Sshunt", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Tcell", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamTkI", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamTkU", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamTyp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Tenv", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Tmonicell", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Insol", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("InsolMpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CorrectedToInsol", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellIDStr", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("IRevmax", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RserLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RshuntLf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("MonicellMvToInsol", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("FlashMonicellVoltage", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2Class", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2ContactIssue", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2CrackDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2CrystalDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2CrystalDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2CrystalDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2DarkDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2DarkDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2DarkDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2DarkGrainBoundary", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2EdgeBrick", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerContinuousDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerContinuousDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerContinuousDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2Firing", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2GripperMark", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2Line", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2OxRing", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2ScratchDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2ScratchDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2ScratchDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2SpotDefaultArea", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2SpotDefaultCount", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2SpotDefaultSeverity", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELBin", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELBinComment", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELCamExposureTime", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELCamGain", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2EvalRecipe", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("PeCon", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("PeCTsys", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("PeCYT", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("PePMT", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("PePreviousPINotReadyForMeasurement", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2TimeEvaluation", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Isc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Uoc", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Pmpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_FF", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Eta", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Insol", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Pmpp", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Insol", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Insol_M1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M2_Insol_M2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Insol_M1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Insol_M2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Insol_M1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Insol_M2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("eqp_id", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("AOI_1_Q", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("AOI_1_R", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("AOI_2_Q", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("AOI_2_R", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("IRev2", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("IRev1", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RshuntDr", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RshuntDf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RserLfDf", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN_Comment", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELMeanGray", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("RshuntDfDr", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestTime", "")),
        tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestDate", "")),
        order_type,
        bin_type,
        factory
      )
    }
    halm2
  }
}





