package com.example.logprocessor

import org.apache.spark.sql.SparkSession

/**
 *
 */
object App {
    
  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Log Processor Kafka")
      .getOrCreate();
    println("Spark Version : " + spark.version);

    val df = spark
      .read
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "connexion_logs")
      .option("startingOffsets", "earliest")
      .load();
    
    df.printSchema();

    df.show();

    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    df2.show(false);
  }

}
