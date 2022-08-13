package com.coffeeco.data

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, StreamingQuery}

object SparkRedisStreamsApp extends SparkApplication {
  val logger: Logger = Logger.getLogger("com.coffeeco.data.SparkRedisStreamsApp")

  override def validateConfig()(implicit sparkSession: SparkSession): Boolean = {
    sparkConf.get("spark.app.source.stream", "").nonEmpty
  }

  lazy val inputStreamName: String = sparkConf.get(
    "spark.app.source.stream","com:coffeeco:coffee:v1:orders")

  // stream data source format for reading
  lazy val streamStruct: StructType = new StructType()
    .add(StructField("timestamp", LongType, nullable = false))
    .add(StructField("orderId", StringType, nullable = false))
    .add(StructField("storeId", StringType, nullable = false))
    .add(StructField("customerId", StringType, nullable = false))
    .add(StructField("numItems", IntegerType, nullable = false))
    .add(StructField("price", FloatType, nullable = false))
  
  // data stream source reader
  lazy val inputStream: DataStreamReader = {
    sparkSession.readStream
      .format("redis")
      .option("stream.keys", inputStreamName)
      .schema(streamStruct)
  }

  /**
   * Simple Streaming App: Connects and process as soon as data is available
   * - all input will be output to the console and stored no where
   */
  override def run(): Unit = {
    super.run()
    
    val writer: DataStreamWriter[Row] = SparkRedisStreams(sparkSession)
      .transform(inputStream.load())
      .writeStream
      .queryName("orders")
      .format("console")

    startAndAwaitApp(writer.start())
  }

  def startAndAwaitApp(query: StreamingQuery): Unit = {
    query.awaitTermination()
  }

  run()

}