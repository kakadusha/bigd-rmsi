package ru.ertelecom.kafka.extract.netflow.serde

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.netflow.domain.NetflowMock

class ToStringDeserializer(appConf: Config) extends Deserializer(appConf){
  override def deserialize(): UserDefinedFunction = udf{ payload: Array[Byte] =>
    NetflowMock(new String(payload))
  }
}
