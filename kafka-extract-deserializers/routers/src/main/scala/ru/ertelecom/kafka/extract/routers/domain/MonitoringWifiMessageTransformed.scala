package ru.ertelecom.kafka.extract.routers.domain

import proto.message.DataWiFi
import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class MonitoringWifiMessageTransformed(monitoringDate: java.sql.Date,
                                            monitoringTimestamp: Long,
                                            deviceId: Long,
                                            hostmodelId: Int,
                                            terminalResource: String,
                                            methodName: String,
                                            city: String,
                                            monitoringTaskId: String,
                                            data: Seq[DataWiFi],
                                            year: Int,
                                            month: Int,
                                            day: Int) extends SerializableMessage {

}
