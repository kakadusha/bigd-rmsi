package ru.ertelecom.kafka.extract.marker.domain

case class MarkerParsed(
                         triggered_date: String,
                         triggered_ts: Long,
                         billing_id: Int,
                         login: String,
                         marker: String
                       )
