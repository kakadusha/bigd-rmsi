package ru.ertelecom.dpi.eql.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class EquilaDpiCampaignLog(
                               billing_id: Int,
                               box_id: Int,
                               ts: Long,
                               login: String,
                               device_id: String,
                               client_private_ip: Long,
                               client_request_ip: Long,
                               url: String,
                               ua: String,
                               campaign_id: Long,
                               event_type: Int,
                               sid: String,
                               referer: String,
                               msgType: Option[Int],
                               for_day: String
                               ) extends SerializableMessage {}
