package com.example.logprocessor

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * A base class for Spark-related test setups. Manages the creation and destruction of a SparkSession for testing.
 */
class SparkSessionTestBase extends AnyFunSuite with BeforeAndAfterAll {

  val master = "local[*]"
  var sparkSession: SparkSession = _

  /**
   * Sets up the SparkSession before all tests.
   */
  override def beforeAll(): Unit = {
    println("Setting up SparkSession for tests...")
    sparkSession = SparkSession
      .builder()
      .appName("Log Processor Kafka")
      .master(master)
      .getOrCreate()
  }

  /**
   * Tears down the SparkSession after all tests.
   */
  override def afterAll(): Unit = {
    println("Tearing down SparkSession after tests...")
    sparkSession.stop()
  }
}
