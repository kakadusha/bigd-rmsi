package ru.ertelecom.kafka.extract.sms.util

import play.api.libs.json.{JsLookupResult, JsNumber, JsString}

object ParseUtils {
  def parseBid(lookup: JsLookupResult): Option[Int] = {
    lookup.toOption.flatMap(s => {
      s match {
        case JsString(value) => Some(value.toInt)
        case JsNumber(value) => Some(value.toIntExact)
        case _ => Option.empty
      }
    })
  }
}
