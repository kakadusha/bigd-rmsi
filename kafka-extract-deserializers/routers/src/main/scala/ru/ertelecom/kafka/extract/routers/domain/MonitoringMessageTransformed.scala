package ru.ertelecom.kafka.extract.routers.domain

import proto.message.Data
import ru.ertelecom.kafka.extract.core.serde.SerializableMessage


case class MonitoringMessageTransformed(
                                         monitoringDate: String,
                                         monitoringTimestamp: Long,
                                         deviceId: Long,
                                         hostmodelId: Int,
                                         terminalResource: String,
                                         methodName: String,
                                         city: String,
                                         monitoringTaskId: String,
                                         mask: String,
                                         branch: String,
                                         metricValue: String,
                                         for_day: String
                                       ) extends SerializableMessage{

}
