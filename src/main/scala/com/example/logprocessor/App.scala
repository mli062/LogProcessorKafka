package com.example.logprocessor

import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Log Processor Kafka")
      .getOrCreate()

    // Print Spark version for reference
    println("Spark Version: " + spark.version)

    // Read streaming data from Kafka topic
    val kafkaConsumer = spark
      .readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "connexion_logs")
      .option("startingOffsets", "earliest")
      .load()

    // Log Processing
    val connectionLogStrDF = kafkaConsumer.selectExpr("CAST(value AS STRING)")
    val parsedConnectionLogDF = LogProcessor.parseConnectionLog(connectionLogStrDF)

    // Analytics
    val connectionStatusCountDF = LogProcessor.connectionStatusCount(parsedConnectionLogDF)

    // Write streaming analytics to the console
    val query = connectionStatusCountDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    // Await termination to keep the application running
    query.awaitTermination()
  }

}