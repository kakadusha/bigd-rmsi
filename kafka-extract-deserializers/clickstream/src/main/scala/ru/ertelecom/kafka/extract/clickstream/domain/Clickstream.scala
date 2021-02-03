package ru.ertelecom.kafka.extract.clickstream.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class Clickstream(
                        timestamp_local: Long,
                        timestamp_utc: Long,
                        login: String,
                        ip: String,
                        url: String,
                        ua: String,
                        city_id: Short,
                        date_local: String,
                        cookie1: Option[String],
                        cookie2: Option[String],
                        cookie3: Option[String],
                        browser_name: Option[String],
                        browser_version: Option[String],
                        os_name: Option[String],
                        os_version: Option[String],
                        device: Option[String],
                        mac: Option[String],
                        router_manufacturer: Option[String]
                      ) extends SerializableMessage {}
