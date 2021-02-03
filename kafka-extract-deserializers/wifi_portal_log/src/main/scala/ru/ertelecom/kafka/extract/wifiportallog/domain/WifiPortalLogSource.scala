package ru.ertelecom.kafka.extract.wifiportallog.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WifiPortalLogSource(
    ts: Long,
    except_message: String,
    except_name: String,
    endpoint: String,
    http_code: Option[Int],
    mac: Option[String],
    ip: Option[String],
    vlan: Option[String],
    domain_tags: Option[String],
    domain_user: Option[String],
    session: Option[String],
    city_id_domru: Option[Int],
    city_id_tags: Option[String],
    city_id_user: Option[String],
    page_id: Option[String],
    user_name: Option[String],
    nas_ip: Option[String],
    radius_attrs: Option[List[Map[String, String]]],
    location_id: Option[String]
    ) {}

object WifiPortalLogSource{
  import play.api.libs.functional.syntax._
  import play.api.libs.json._
  implicit val wifiPortalLogSourceReads: Reads[WifiPortalLogSource] = (
    (JsPath \ "timestamp").read[Long] and
    (JsPath \ "exeptionMessage").read[String] and
    (JsPath \ "tags" \ "name").read[String] and
    (JsPath \ "tags" \ "endPoint").read[String] and
    (JsPath \ "tags" \ "code").readNullable[Int] and
    (JsPath \ "tags" \ "mac").readNullable[String] and
    (JsPath \ "tags" \ "ip").readNullable[String] and
    (JsPath \ "tags" \ "vlan").readNullable[String] and
    (JsPath \ "tags" \ "domain").readNullable[String] and
    (JsPath \ "user" \ "domain").readNullable[String] and
    (JsPath \ "user" \ "key").readNullable[String] and
    ((JsPath \ "user" \ "DomRuPixel" \ "CityId").readNullable[Int] or Reads.pure(None:Option[Int])) and
    (JsPath \ "tags" \ "domain").readNullable[String] and
    (JsPath \ "user" \ "domain").readNullable[String] and
    ((JsPath \ "user" \ "DomRuPixel" \ "PageId").readNullable[String] or Reads.pure(None:Option[String])) and
    ((JsPath \ "user" \ "DomRuPixel" \ "UserId").readNullable[String] or Reads.pure(None:Option[String])) and
    (JsPath \ "user" \ "nas_ip").readNullable[String] and
    (JsPath \ "user" \ "radius_attrs").readNullable[List[Map[String, String]]] and
    ((JsPath \ "user" \ "DomRuPixel" \ "LocationId").readNullable[String] or Reads.pure(None:Option[String]))
    ) (WifiPortalLogSource.apply _)
}
