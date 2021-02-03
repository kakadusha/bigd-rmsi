package ru.ertelecom.kafka.extract.recommendations.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.transform.Transform

class UnnestArrayTransform(appConf: Config) extends Transform(appConf) {
  override def transform(dfIn: DataFrame): DataFrame = {
    dfIn.withColumn("exploded", explode(col("value_deserialized")))
      .drop("value_deserialized")
      .withColumnRenamed("exploded", "value_deserialized")
  }
}
