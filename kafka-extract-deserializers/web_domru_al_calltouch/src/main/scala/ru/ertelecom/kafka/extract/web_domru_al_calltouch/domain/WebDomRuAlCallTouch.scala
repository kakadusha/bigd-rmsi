package ru.ertelecom.kafka.extract.web_domru_al_calltouch.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WebDomRuAlCallTouch(
                            ts: Long,
                            ip: Long,
                            callphase: Option[String] = None,
                            callerphone: Option[String] = None, // Long,
                            phonenumber: Option[String] = None, // Long,
                            redirectNumber: Option[String] = None,
                            source: Option[String] = None,
                            medium: Option[String] = None,
                            utm_source: Option[String] = None,
                            utm_medium: Option[String] = None,
                            utm_campaign: Option[String] = None,
                            utm_term: Option[String] = None,
                            gcid: Option[String] = None,
                            yaClientId: Option[String] = None,
                            sessionId: Option[String] = None, // Long
                            hostname: Option[String] = None,
                            ourl: Option[String] = None,
                            referer: Option[String] = None,
                            user_agent: Option[String] = None,
                            for_day: String = "2000-01-01"
                          ) extends SerializableMessage
