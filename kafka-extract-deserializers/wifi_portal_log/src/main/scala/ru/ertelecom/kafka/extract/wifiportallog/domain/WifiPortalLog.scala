package ru.ertelecom.kafka.extract.wifiportallog.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WifiPortalLog(
    ts: Long,
    except_message: String,
    except_code: Int,
    except_name: String,
    http_code: Option[Int],
    endpoint: String,
    mac: Option[String],
    ip: Option[Long],
    vlan: Option[String],
    domain: Option[String],
    session: Option[String],
    city_id: Option[Int],
    page_id: Option[String],
    user_name: Option[String],
    nas_ip: Option[Long],
    filter: Option[String],
    profile: Option[String],
    timeout: Option[Int]
    ) extends SerializableMessage {}