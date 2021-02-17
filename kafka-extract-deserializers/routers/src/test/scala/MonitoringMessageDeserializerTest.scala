import org.scalatest.FunSuite
import proto.message.{Data, MonitoringMessage}
import ru.ertelecom.kafka.extract.routers.domain.MonitoringMessageTransformed
import ru.ertelecom.kafka.extract.routers.serde.MonitoringMessageDeserializer

class MonitoringMessageDeserializerTest extends FunSuite {

  test("with iso dates") {
    val desrializer = new MonitoringMessageDeserializer(null)
    val inMsg = MonitoringMessage(
      monitoringDate = "2011-12-03",
      monitoringTimestamp = "2011-12-03T10:15:30+01:00",
      deviceId = 100L,
      hostmodelId = 12,
      terminalResource = "v123456",
      methodName = "telnet",
      city = "chel",
      monitoringTaskId = "monitoringInChel1",
      data = Seq(
        Data(
          mask = "mask1",
          branch = "branch1",
          value = "123.4"
        ),
        Data(
          mask = "mask2",
          branch = "branch2",
          value = "100500"
        )
      )
    )
    val expected = Array(
      MonitoringMessageTransformed(
        monitoringDate = "2011-12-03",
        monitoringTimestamp = 1322903730000L,
        deviceId = 100L,
        hostmodelId = 12,
        terminalResource = "v123456",
        methodName = "telnet",
        city = "chel",
        monitoringTaskId = "monitoringInChel1",
        mask = "mask1",
        branch = "branch1",
        metricValue = "123.4",
        for_day = "2011-12-03"
      ),
      MonitoringMessageTransformed(
        monitoringDate = "2011-12-03",
        monitoringTimestamp = 1322903730000L,
        deviceId = 100L,
        hostmodelId = 12,
        terminalResource = "v123456",
        methodName = "telnet",
        city = "chel",
        monitoringTaskId = "monitoringInChel1",
        mask = "mask2",
        branch = "branch2",
        metricValue = "100500",
        for_day = "2011-12-03"
      )
    )
    val actual = desrializer.convertMonMessage(inMsg)
    assert(expected sameElements actual)
  }

  test("with rfc dates") {
    val desrializer = new MonitoringMessageDeserializer(null)
    val inMsg = MonitoringMessage(
      monitoringDate = "2011-12-03T10:15:30.179780629+01:00",
      monitoringTimestamp = "2011-12-03T10:15:30.179780629+01:00",
      deviceId = 100L,
      hostmodelId = 12,
      terminalResource = "v123456",
      methodName = "telnet",
      city = "chel",
      monitoringTaskId = "monitoringInChel1",
      data = Seq(
        Data(
          mask = "mask1",
          branch = "branch1",
          value = "123.4"
        ),
        Data(
          mask = "mask2",
          branch = "branch2",
          value = "100500"
        )
      )
    )
    val expected = Array(
      MonitoringMessageTransformed(
        monitoringDate = "2011-12-03",
        monitoringTimestamp = 1322903730000L,
        deviceId = 100L,
        hostmodelId = 12,
        terminalResource = "v123456",
        methodName = "telnet",
        city = "chel",
        monitoringTaskId = "monitoringInChel1",
        mask = "mask1",
        branch = "branch1",
        metricValue = "123.4",
        for_day = "2011-12-03"
      ),
      MonitoringMessageTransformed(
        monitoringDate = "2011-12-03",
        monitoringTimestamp = 1322903730000L,
        deviceId = 100L,
        hostmodelId = 12,
        terminalResource = "v123456",
        methodName = "telnet",
        city = "chel",
        monitoringTaskId = "monitoringInChel1",
        mask = "mask2",
        branch = "branch2",
        metricValue = "100500",
        for_day = "2011-12-03"
      )
    )
    val actual = desrializer.convertMonMessage(inMsg)
    assert(expected sameElements actual)
  }

}
