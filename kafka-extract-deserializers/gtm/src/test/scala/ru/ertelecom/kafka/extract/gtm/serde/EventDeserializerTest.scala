package ru.ertelecom.kafka.extract.gtm.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.gtm.domain.Event

class EventDeserializerTest extends FunSuite{

    test("success") {
        val input = """/gtm?eventName=UAevent&evts=1605602921006&t1id=98&t1st=success&t1et=73&t2id=90&t2st=success&t2et=1&t3id=543&t3st=success&t3et=0	https://yar.domru.ru/promo""".stripMargin
        val payload = input.getBytes
        val deserializer = new EventDeserializer(null)
        val answersArray = deserializer.parseTsv(payload).get
        assert(answersArray.nonEmpty)
        assert(3 == answersArray.length)
        assert("UAevent" == answersArray(0).event_name)
    }

    test("test undefined") {
        val input = """/gtm?eventName=UAevent&evts=1605602921006&t1id=98&t1st=success&t1et=undefined&t2id=90&t2st=success&t2et=1&t3id=543&t3st=success&t3et=0	https://yar.domru.ru/promo""".stripMargin
        val payload = input.getBytes
        val deserializer = new EventDeserializer(null)
        val answersArray = deserializer.parseTsv(payload).get
        assert(answersArray.nonEmpty)
        assert(3 == answersArray.length)
        assert("UAevent" == answersArray(0).event_name)
        assert(0L == answersArray(0).tag_estime)
    }
}
