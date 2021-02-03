package ru.ertelecom.kafka.extract.gtm.serde

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer

import ru.ertelecom.kafka.extract.gtm.domain.Event

class EventDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

    override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
        this.parseTsv(payload)
    }

    def parseTsv(payload: Array[Byte]): Option[Array[Event]] = {
        try {
            var result: Array[Event] = Array()

            val it = new String(payload)
            val partParams = it.split("\\t")(0)
            val partRefs = if (it.split("\\t").length > 1) it.split("\\t")(1) else "empty"

            val paramsRaw = partParams.split("\\?")(1).split("&")
            val eventName = paramsRaw(0).split("=")(1)
            val eventTs = paramsRaw(1).split("=")(1).toLong

            var ind = 2
            while (ind < paramsRaw.length) {
                result :+= Event(
                    event_name = eventName,
                    event_ts = eventTs,
                    tag_id = if (paramsRaw(ind).split("=").length > 1) paramsRaw(ind).split("=")(1) else "empty",
                    tag_status = if (paramsRaw(ind + 1).split("=").length > 1) paramsRaw(ind + 1).split("=")(1) else "empty",
                    tag_estime = if (paramsRaw(ind + 2).split("=").length > 1 && !paramsRaw(ind + 2).split("=")(1).contains("undefined")) paramsRaw(ind + 2).split("=")(1).toLong.abs else 0L,
                    ref = partRefs
                )
                ind += 3
            }
            Option.apply(result)
        } catch {
            case e: Exception => Option.empty
        }
    }
}
