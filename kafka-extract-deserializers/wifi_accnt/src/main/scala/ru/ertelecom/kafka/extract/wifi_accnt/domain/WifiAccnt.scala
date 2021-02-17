package ru.ertelecom.kafka.extract.wifi_accnt.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WifiAccnt
(`User-Name`: String,
 `NAS-Identifier`: String,
 `Acct-Status-Type`: Int,
 `Acct-Session-Id`: String,
 `Acct-Input-Octets`: Long,
 `Acct-Input-Gigawords`: Long,
 `Acct-Output-Octets`: Long,
 `Acct-Output-Gigawords`: Long,
 `Calling-Station-ID`: Option[String],
 `Framed-IP-Address`: String,
 `Acct-Session-Time`: Long,
 `Event-Timestamp`: Long,
 `ADSL-Agent-Circuit-Id`: Option[String],
 `Alc-SLA-Prof-Str`: Option[String],
 `Acct-Terminate-Cause`: Int,
 `Acct-Multi-Session-Id`: Option[String],
 `NAS-Port-Id`: Option[String],
 `NAS-IP-Address`: String,
 `NAS-port-Type`: String,
 `Huawei-Domain-Name`: Option[String],
 `Huawei-Policy-Name`: Option[String],
 `Alc-Client-Hardware-Addr`: Option[String],
 `Wifi-sub`: Int,
 `auth_id`: Int,
 `vlan`: String,
 `city_id`: Int,
 `duration`: Long,
 `input_bytes`: Long,
 `output_bytes`: Long,
 `start_time`: Long,
 `stop_time`: Long
)
  extends SerializableMessage {

}
