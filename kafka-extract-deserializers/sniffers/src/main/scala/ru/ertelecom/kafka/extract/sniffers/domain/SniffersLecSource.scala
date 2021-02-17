package ru.ertelecom.kafka.extract.sniffers.domain

case class SniffersLecSource(sensor: String,
                             col: Option[Array[Array[NastedArrayValue]]]){

}
object SniffersLecSource {

  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  private implicit val nastedArrayValueReads: Reads[NastedArrayValue] = new Reads[NastedArrayValue]{
    override def reads(json: JsValue): JsResult[NastedArrayValue] = {
      json match {
        case JsNumber(num) => JsSuccess(IntNastedArrayValue(num.toInt))
        case JsString(s) => JsSuccess(StringNastedArrayValue(s))
        case _ => JsError("Parsing error")
      }
    }
  }

  implicit val sniffersLecSourceReads: Reads[SniffersLecSource] = (
    (JsPath \ "mac").read[String] and
      (JsPath \ "col").readNullable[Array[Array[NastedArrayValue]]]
    ) (SniffersLecSource.apply _)

}
