package com.example.logprocessor

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class LogProcessorTest extends SparkSessionTestBase with DataFrameSuiteBase {

  var testConnectionLogDF: Dataset[Row] = _
  var expectedParseDataDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll() // Call the beforeAll method from the base class

    // Connection Log from Kafka

    val data = Seq(
      Row("Jan 18 14:35:14 HOSTNAME sshd[6055818]: Failed password for invalid user fanvzlys from 92.120.4.88 port 443 ssh2"),
      Row("Jan 18 14:38:34 HOSTNAME sshd[6285637]: 'Accepted' password for authorized_user2 from 185.7.73.102 port 22 ssh2\nJan 18 14:38:34 HOSTNAME sshd[6285637]: pam_unix(sshd:session): session opened for user authorized_user2(uid=0) by (uid=0)\nJan 18 14:38:34 HOSTNAME systemd-logind[6285637]: New session 1 of user authorized_user2."),
      Row("Jan 18 14:42:14 HOSTNAME sshd[6055818]: Failed password for root from 185.7.73.102 port 22 ssh2")
    )

    val schema = List(
      StructField("value", StringType, nullable = false)
    )

    testConnectionLogDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data),
      StructType(schema)
    )

    // Parse Connection Log

    val expectedParseData = Seq(
      Row("1970-01-18 14:35:14", "fanvzlys", "92.120.4.88", 443, false, false),
      Row("1970-01-18 14:38:34", "authorized_user2", "185.7.73.102", 22, true, true),
      Row("1970-01-18 14:42:14", "root", "185.7.73.102", 22, false, true)
    )

    val parsedConnectionLogSchema = List(
      StructField("timestamp", StringType, nullable = true),
      StructField("username", StringType, nullable = false),
      StructField("ip", StringType, nullable = false),
      StructField("port", IntegerType, nullable = true),
      StructField("connection_status", BooleanType, nullable = false),
      StructField("user_existence", BooleanType, nullable = false)
    )

    expectedParseDataDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(expectedParseData),
      StructType(parsedConnectionLogSchema)
    )
  }

  test("testParseConnectionLog") {
    val outputParseConnectionLogDF = LogProcessor.parseConnectionLog(testConnectionLogDF)
    assertDataFrameEquals(expectedParseDataDF, outputParseConnectionLogDF)
  }

  test("testConnectionStatusCount") {
   val expectedConnectionStatusCount = Seq(
      Row(2L, 1L, 1L),
      Row(1L, 0L, 1L),
   )

    val connectionStatusCountSchema = StructType(Seq(
      StructField("total_connections", LongType, nullable = false),
      StructField("successful_connections", LongType, nullable = false),
      StructField("failed_connections", LongType, nullable = true)
    ))

    val expectedConnectionStatusCountDF = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(expectedConnectionStatusCount),
      StructType(connectionStatusCountSchema)
    ).select("total_connections", "successful_connections", "successful_connections")

    val outputConnectionStatusCountDF = LogProcessor.connectionStatusCount(expectedParseDataDF)
      .select("total_connections", "successful_connections", "successful_connections")

    assertDataFrameEquals(expectedConnectionStatusCountDF, outputConnectionStatusCountDF)
  }

  test("testRegexTimestamp") {
    val log = "Jan 18 14:35:14 HOSTNAME sshd[6055818]: Failed password for invalid user fanvzlys from 92.120.4.88 port 443 ssh2"
    val timestampRegexResult = LogProcessor.timestampRegex.findFirstMatchIn(log)

    timestampRegexResult shouldBe defined

    val timestampValue = timestampRegexResult.get.group(1)
    timestampValue shouldBe "Jan 18 14:35:14"
  }

  test("testRegexUsername") {
    val log = "Jan 18 14:35:14 HOSTNAME sshd[6055818]: Failed password for invalid user fanvzlys from 92.120.4.88 port 443 ssh2"
    val usernameRegexResult = LogProcessor.usernameRegex.findFirstMatchIn(log)

    usernameRegexResult shouldBe defined

    val usernameValue = usernameRegexResult.get.group(2)
    usernameValue shouldBe "fanvzlys"
  }

  test("testRegexIp") {
    val log = "Jan 18 14:35:14 HOSTNAME sshd[6055818]: Failed password for invalid user fanvzlys from 92.120.4.88 port 443 ssh2"
    val ipRegexResult = LogProcessor.ipRegex.findFirstMatchIn(log)

    ipRegexResult shouldBe defined

    val ipValue = ipRegexResult.get.group(1)
    ipValue shouldBe "92.120.4.88"
  }

  test("testRegexPort") {
    val log = "Jan 18 14:35:14 HOSTNAME sshd[6055818]: Failed password for invalid user fanvzlys from 92.120.4.88 port 443 ssh2"
    val portRegexResult = LogProcessor.portRegex.findFirstMatchIn(log)

    portRegexResult shouldBe defined

    val portValue = portRegexResult.get.group(1)
    portValue shouldBe "443"
  }

}