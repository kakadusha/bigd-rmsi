package ru.ertelecom.kafka.extract.rmsi.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class RmsiMetrics(
                        serviceName: String,
                        entityType: String,
                        createTime: Long,
                        lastUpdate: Long,
                        billing: String,
                        message: Option[String],
                        errorMessage: Option[String],
                        status: Option[String],
                        result: Option[String],
                        login: Option[String],
                        networkActionType: Option[String],
                        networkActionTypeName: Option[String],
                        identificationType: Option[String],
                        houseId: Option[String],
                        nodeId: Option[String],
                        switchId: Option[String],
                        elementId: Option[String],
                        resourceId: Option[String],
                        network: Option[String],
                        ports: Option[String],
                        parameters: Option[String],
                        operationType: Option[String],
                        realLogin: Option[String],
                        impersonatedLogin: Option[String],
                        errorHistory: Option[Array[String]],
                        orderId: Option[String],
                        requestId: Option[String],
                        companyId: Option[String],
                        office: Option[String],
                        packageId: Option[String],
                        agreementNumber: Option[String],
                        timeSlot: Option[String],
                        comment: Option[String],
                        transferAction: Option[String],
                        accessPanelId: Option[String],
                        screenshotLink: Option[String],
                        territories: Option[String]
) extends SerializableMessage {

}
