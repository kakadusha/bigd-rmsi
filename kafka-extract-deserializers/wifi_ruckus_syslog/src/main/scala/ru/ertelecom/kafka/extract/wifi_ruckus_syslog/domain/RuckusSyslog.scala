package ru.ertelecom.kafka.extract.wifi_ruckus_syslog.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class RuckusSyslog(
    log_entry_date: String,
    ts: Long,
    ssid: String,
    event_type: String,
    ap_mac: String,
    client_mac: String,
    client_ip: Option[Long],
    username: Option[String],
    vlan: Int,
    client_os: Option[String],
    client_hostname: Option[String],
    radio: String,
    first_auth: Option[Long],
    association_time: Option[Long],
    ip_assign_time: Option[Long],
    disconnect_time: Option[Long],
    session_duration: Option[Long],
    disconnect_reason: Option[Int],
    rx_bytes: Option[Long],
    tx_bytes: Option[Long],
    peak_rx: Option[Int],
    peak_tx: Option[Int],
    rssi: Option[Int],
    received_signal_strength: Option[Int],
    zone_name: String,
    ap_location: Option[String],
    ap_gps: Option[String],
    ap_ip: Long,
    ap_desc: Option[String],
    to_radio: Option[String],
    from_ap_mac: Option[String]
    ) extends SerializableMessage {}