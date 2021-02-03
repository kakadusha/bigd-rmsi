package ru.ertelecom.kafka.extract.tv_device

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.transform.Transform

class TvDeviceTransform(appConf: Config) extends Transform(appConf) {
  override def transform(dfIn: DataFrame): DataFrame = {
    val df = dfIn.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").persist()

    val resultDf = df.select(
      unix_timestamp(regexp_extract(col("value"), "@timestamp=([^\t]+)", 1), "yyyy-MM-dd'T'HH:mm:ssXXX").as("ts"),
      regexp_extract(col("value"), "server.name=([^\t]+)", 1).as("server_name"),
      regexp_extract(col("value"), "server.point=([^\t]+)", 1).as("server_point"),
      regexp_extract(col("value"), "server.type=([^\t]+)", 1).as("server_type"),
      regexp_extract(col("value"), "server.number_connection=([^\t]+)", 1).as("server_num_conn").cast(LongType),
      regexp_extract(col("value"), "server.connection_requests=([^\t]+)", 1).as("server_conn_req").cast(IntegerType),
      unix_timestamp(regexp_extract(col("value"), "request.time=([^\t]+)", 1), "dd/MMM/yyyy:HH:mm:ss Z").as("req_time"),
      regexp_extract(col("value"), "request.sxema=([^\t]+)", 1).as("req_schema"),
      regexp_extract(col("value"), "request.metod=([^\t]*)", 1).as("req_method"),
      regexp_extract(col("value"), "request.host=([^\t]+)", 1).as("req_host"),
      regexp_extract(col("value"), "request.block_location=([^\t]+)", 1).as("req_block_location"),
      regexp_extract(col("value"), "request.block_location_name=([^\t]+)", 1).as("req_block_location_name"),
      regexp_extract(col("value"), "request.location=([^\t]+)", 1).as("req_location"),
      regexp_extract(col("value"), "request.real_url=([^\t]+)", 1).as("req_real_url"),
      regexp_extract(col("value"), "request.internal_url=([^\t]+)", 1).as("req_internal_url"),
      regexp_extract(col("value"), "request.body=([^\t]+)", 1).as("req_body"),
      regexp_extract(col("value"), "request.body_byte_in=([^\t]+)", 1).as("req_body_byte_in"),
      regexp_extract(col("value"), "request.ip=([^\t]+)", 1).as("req_ip"),
      regexp_extract(col("value"), "request.port=([^\t]+)", 1).as("req_port").cast(IntegerType),
      regexp_extract(col("value"), "request.user=([^\t]+)", 1).as("req_user"),
      regexp_extract(col("value"), "request.referer=([^\t]+)", 1).as("req_referer"),
      regexp_extract(col("value"), "request.agent=([^\t]+)", 1).as("req_agent"),
      regexp_extract(col("value"), "request.forward=([^\t]+)", 1).as("req_forward"),
      regexp_extract(col("value"), "request.protocol=([^\t]+)", 1).as("req_protocol"),
      regexp_extract(col("value"), "request.x_app_version=([^\t]+)", 1).as("req_app_version"),
      regexp_extract(col("value"), "response.code=([^\t]+)", 1).as("resp_code").cast(IntegerType),
      regexp_extract(col("value"), "response.body_byte_sent=([^\t]+)", 1).as("resp_body_byte_sent").cast(IntegerType),
      regexp_extract(col("value"), "response.time=([^\t]+)", 1).as("resp_time").cast(DoubleType),
      regexp_extract(col("value"), "response.upstream_addrs=([^\t]+)", 1).as("resp_upstream_addrs"),
      regexp_extract(col("value"), "response.upstream_conn_time=([^\t]+)", 1).as("resp_upstream_conn_time").cast(DoubleType),
      regexp_extract(col("value"), "response.upstream_header_time=([^\t]+)", 1).as("resp_upstream_header_time").cast(DoubleType),
      regexp_extract(col("value"), "response.upstream_response_time=([^\t]+)", 1).as("resp_upstream_resp_time").cast(DoubleType),
      regexp_extract(col("value"), "response.upstream_status_code=([^\t]+)", 1).as("resp_upstream_status_code").cast(IntegerType),
      regexp_extract(col("value"), "response.cache_status=([^\t]+)", 1).as("resp_cache_status").cast(IntegerType),
      regexp_extract(col("value"), "response.cache_group=([^\t]+)", 1).as("resp_cache_group"),
      regexp_extract(col("value"), "response.upstream_x_result_status=([^\t]+)", 1).as("resp_upstream_x_result_status"),
      regexp_extract(col("value"), "response.upstream_tag_error=([^\t]+)", 1).as("resp_upstream_tag_error"),
      regexp_extract(col("value"), "response.upstream_tag_error_code=([^\t]+)", 1).as("resp_upstream_tag_error_code").cast(IntegerType),
      regexp_extract(col("value"), "response.lua_response=([^\t]+)", 1).as("resp_lua_response").cast(IntegerType),
      regexp_extract(col("value"), "platform.id=([^\t]+)", 1).as("platform_id").cast(IntegerType),
      regexp_extract(col("value"), "platform.extid=([^\t]+)", 1).as("platform_extid"),
      regexp_extract(col("value"), "subscriber.is_guest=([^\t]+)", 1).as("subscriber_is_guest").cast(BooleanType),
      regexp_extract(col("value"), "subscriber.id=([^\t]+)", 1).as("subscriber_id").cast(IntegerType),
      regexp_extract(col("value"), "subscriber.extid=([^\t]+)", 1).as("subscriber_extid"),
      regexp_extract(col("value"), "subscriber.token_type=([^\t]+)", 1).as("subscriber_token_type"),
      regexp_extract(col("value"), "subscriber.token_is_expires=([^\t]+)", 1).as("subscriber_token_is_expires").cast(BooleanType),
      regexp_extract(col("value"), "subscriber.operator_id=([^\t]+)", 1).as("subscriber_operator_id").cast(IntegerType),
      regexp_extract(col("value"), "subscriber.operator_extid=([^\t]+)", 1).as("subscriber_operator_extid"),
      regexp_extract(col("value"), "subscriber.groups_title=([^\t]+)", 1).as("subscriber_groups_title"),
      regexp_extract(col("value"), "subscriber.groups_id=([^\t]+)", 1).as("subscriber_groups_id"),
      regexp_extract(col("value"), "device.id=([^\t]+)", 1).as("device_id"),
      regexp_extract(col("value"), "device.extid=([^\t]+)", 1).as("device_extid")
    )
      .withColumn("year", year(from_unixtime(col("ts"))))
      .withColumn("month", month(from_unixtime(col("ts"))))
      .withColumn("day", dayofmonth(from_unixtime(col("ts"))))
    df.unpersist()
    return resultDf
  }
}
