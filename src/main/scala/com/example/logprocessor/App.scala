package com.example.logprocessor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, count, from_unixtime, unix_timestamp, when, window, regexp_extract}
import org.apache.spark.sql.types.IntegerType


object App {

  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Log Processor Kafka")
      .getOrCreate();
    println("Spark Version : " + spark.version);

    val kafkaConsumer = spark
      .readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "connexion_logs")
      .option("startingOffsets", "earliest")
      .load()
    
    val connectionLogStrDF = kafkaConsumer.selectExpr("CAST(value AS STRING)")

    val timestampRegex = """(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})""".r
    val usernameRegex = """password for (invalid user\s+)?(\w+) from""".r
    val ipRegex = """from\s+([\d\.]+)\s+port""".r
    val portRegex = """port\s+(\d+)""".r

    val parsedDF = connectionLogStrDF.select(
      from_unixtime(unix_timestamp(regexp_extract(col("value"), timestampRegex.toString, 1), "MMM dd HH:mm:ss")).as("timestamp"),
      regexp_extract(col("value"), usernameRegex.toString(), 2).as("username"),
      regexp_extract(col("value"), ipRegex.toString(), 1).as("ip"),
      regexp_extract(col("value"), portRegex.toString(), 1).cast(IntegerType).as("port"),
      when(col("value").contains("Failed"), false).otherwise(true).as("connection_status"),
      when(col("value").contains("invalid user"), false).otherwise(true).as("user_existence")
    )

    val aggregatedDF = parsedDF
      .groupBy(window(col("timestamp"), "10 minutes").as("Timestamp"))
      .agg(
        count("*").as("total_connections"),
        count(when(col("connection_status"), 1)).as("successful_connections"),
        count(when(!col("connection_status"), 1)).as("failed_connections")
      )
      .orderBy(asc("Timestamp"))

    val query = aggregatedDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}