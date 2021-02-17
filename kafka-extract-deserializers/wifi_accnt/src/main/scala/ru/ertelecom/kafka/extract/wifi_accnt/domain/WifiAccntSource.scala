package ru.ertelecom.kafka.extract.wifi_accnt.domain

case class WifiAccntSource(
                            un: String,
                            nasip: String,
                            nasid: String,
                            astype: Int,
                            asid: String,
                            aioct: Long,
                            aigw: Long,
                            aooct: Long,
                            aogw: Long,
                            callstationid: Option[String],
                            framedip: String,
                            astime: Long,
                            eventts: Long,
                            adslacid: Option[String],
                            alcslaprof: Option[String],
                            atcause: Int,
                            amsessionid: Option[String],
                            nasportid: Option[String],
                            hwidname: Option[String],
                            hwipname: Option[String],
                            alcclienthw: Option[String],
                            nasporttype: Int,
                            wifisub: Int,
                            bid: Int
                          ) {}

object WifiAccntSource {

  import play.api.libs.json._
  import play.api.libs.functional.syntax._

  val fields1to16: Reads[(String, String, String, Int, String, Long, Long, Long, Long, Option[String], String, Long, Long, Option[String], Option[String], Int)] = (
    (JsPath \ "un").read[String] and
      (JsPath \ "nasip").read[String] and
      (JsPath \ "nasid").read[String] and
      (JsPath \ "astype").read[Int] and
      (JsPath \ "asid").read[String] and
      (JsPath \ "aioct").read[Long] and
      (JsPath \ "aigw").read[Long] and
      (JsPath \ "aooct").read[Long] and
      (JsPath \ "aogw").read[Long] and
      (JsPath \ "callstationid").readNullable[String] and
      (JsPath \ "framedip").read[String] and
      (JsPath \ "astime").read[Long] and
      (JsPath \ "eventts").read[Long] and
      (JsPath \ "adslacid").readNullable[String] and
      (JsPath \ "alcslaprof").readNullable[String] and
      (JsPath \ "atcause").read[Int]
    ).tupled


  val fields17to24: Reads[(Option[String], Option[String], Option[String], Option[String], Option[String], Int, Int, Int)] = (
    (JsPath \ "amsessionid").readNullable[String] and
      (JsPath \ "nasportid").readNullable[String] and
      (JsPath \ "hwidname").readNullable[String] and
      (JsPath \ "hwipname").readNullable[String] and
      (JsPath \ "alcclienthw").readNullable[String] and
      (JsPath \ "nasporttype").read[Int] and
      (JsPath \ "wifisub").read[Int] and
      (JsPath \ "bid").read[Int]
    ).tupled

  val f: ((String, String, String, Int, String, Long, Long, Long, Long, Option[String], String, Long, Long, Option[String], Option[String], Int),
    (Option[String], Option[String], Option[String], Option[String], Option[String], Int, Int, Int)) => WifiAccntSource = {
    case ((a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p), (a1, b1, c1, d1, e1, f1, g1, h1)) =>
      WifiAccntSource(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, a1, b1, c1, d1, e1, f1, g1, h1)
  }

  implicit val hugeCaseClassReads: Reads[WifiAccntSource] = (
    fields1to16 and fields17to24
    ).apply({ f })
}