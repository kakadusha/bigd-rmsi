package ru.ertelecom.kafka.extract.recommendations.serde

import org.scalatest.FunSuite

class ClientEventsDeserializerTest extends FunSuite{
    test("successDeserializeClientEvent") {
        val payload =
          """
            |{
            |    "id": "7acb7c95-f773-4f12-98e4-3067a139cd04",
            |    "subscriberId": 84467059,
            |    "extIdCity": "ekat",
            |    "groupId": 35042,
            |    "platformId": 36,
            |    "deviceId": 85072820,
            |    "segmentId": null,
            |    "timeStamp": 1569868013,
            |    "item": {
            |        "type": "19",
            |        "ts": 1569867840147,
            |        "assetId": 717255369,
            |        "scheduleProgramId": 2781885,
            |        "itemType": "schedule",
            |        "usecaseId": "liveTvPlayer",
            |        "assetPosition": -1,
            |        "recommendationId": "null",
            |        "changeType": "null",
            |        "isCatchup": false,
            |        "isCatchupStartover": null,
            |        "channelSid": null,
            |        "position": null,
            |        "lcn": 517,
            |        "start": 1569865188342,
            |        "finish": 1569867840147,
            |        "transactionId": null,
            |        "progressValue": null
            |    }
            | }
          """.stripMargin.getBytes
        val deserializer = new ClientEventsDeserializer(null)
        val clientEventOption = deserializer.parseJson(payload)
        assert(clientEventOption.isDefined)
        val clientEvent = clientEventOption.get
        assert("7acb7c95-f773-4f12-98e4-3067a139cd04" == clientEvent.log_id)
        assert(84467059 == clientEvent.subscriberId)
        assert("ekat" == clientEvent.extIdCity)
        assert(35042 == clientEvent.groupId)
        assert(36 == clientEvent.platformId)
        assert(clientEvent.segmentId.isEmpty)
        assert("2019-09-30 18:26:53" == clientEvent.timestam)
        assert(19 == clientEvent.eventType)
        assert(Option.apply(1569867840147L) == clientEvent.ts)
        assert(Option.apply(717255369L) == clientEvent.assetId)
        assert(Option.apply(2781885) == clientEvent.scheduleProgramId)
        assert(Option.apply("schedule") == clientEvent.itemType)
        assert(Option.apply("liveTvPlayer") == clientEvent.useCaseId)
        assert(Option.apply(-1) == clientEvent.assetPosition)
        assert(clientEvent.recommendationId.isEmpty)
        assert(clientEvent.changeType.isEmpty)
        assert(Option.apply(false) == clientEvent.isCatchup)
        assert(clientEvent.isCatchupStartover.isEmpty)
        assert(clientEvent.channelSid.isEmpty)
        assert(clientEvent.position.isEmpty)
        assert(Option.apply(517) == clientEvent.lcn)
        assert(Option.apply(1569865188342L) == clientEvent.started)
        assert(Option.apply(1569867840147L) == clientEvent.finish)
        assert(clientEvent.transactionId.isEmpty)
        assert(clientEvent.progressValue.isEmpty)
    }

    test("failedDeserializeClientEvent") {
        val payload =
          """{}""".getBytes
        val deserializer = new ClientEventsDeserializer(null)
        val clientEventOption = deserializer.parseJson(payload)
        assert(clientEventOption.isEmpty)
    }

    test("feature: BIGD-869: add for_day") {
        val payload =
          """
            |{
            |    "id": "7acb7c95-f773-4f12-98e4-3067a139cd04",
            |    "subscriberId": 84467059,
            |    "extIdCity": "ekat",
            |    "groupId": 35042,
            |    "platformId": 36,
            |    "deviceId": 85072820,
            |    "segmentId": null,
            |    "timeStamp": 1569868013,
            |    "item": {
            |        "type": "19",
            |        "ts": 1569867840147,
            |        "assetId": 717255369,
            |        "scheduleProgramId": 2781885,
            |        "itemType": "schedule",
            |        "usecaseId": "liveTvPlayer",
            |        "assetPosition": -1,
            |        "recommendationId": "null",
            |        "changeType": "null",
            |        "isCatchup": false,
            |        "isCatchupStartover": null,
            |        "channelSid": null,
            |        "position": null,
            |        "lcn": 517,
            |        "start": 1569865188342,
            |        "finish": 1569867840147,
            |        "transactionId": null,
            |        "progressValue": null
            |    }
            | }
          """.stripMargin.getBytes
        val deserializer = new ClientEventsDeserializer(null)
        val clientEventOption = deserializer.parseJson(payload)
        assert(clientEventOption.isDefined)
        val clientEvent = clientEventOption.get
        assert("2019-09-30" == clientEvent.for_day)
    }
}
