import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite


class MaskingTest extends AnyFunSuite with DataFrameTestToolkit {

  private val spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()

  import spark.implicits._

  test("mask with stars a long credit card number column") {
    val df = Seq("12E4E345XXX64567", "9A8c6D5_").toDF("credit_card_number")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).mask("credit_card_number")
    val expectedDf = Seq("****************", "*******_").toDF("credit_card_number")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("hash with a fixed secret string") {
    AnoToolkit.RANDOM_SECRET = "issamissam123"
    val df = Seq("3 RUE SARIA IBNOU ZOUNAIM").toDF("address")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).hash("address")
    val expectedDf = Seq("887d5b17fc0d53e0f83dfae573342502c81dedab9345c322b5f7ef69f5d2ce19").toDF("address")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("hash with a fixed secret string using a 5 7 substring") {
    AnoToolkit.RANDOM_SECRET = "issamissam123"
    val df = Seq("CPT:2884039").toDF("complexradical")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).hash_substr_5_7("complexradical")
    val expectedDf = Seq("2d4383b302998bcbf4c5758143e5c6aa02868402f56ede9ced43838ddfbf1ab1").toDF("complexradical")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("hash after trimming with a fixed secret string ") {
    AnoToolkit.RANDOM_SECRET = "issamissam123"
    val df = Seq("     2884039          ").toDF("complexradical")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).hash_trim("complexradical")
    val expectedDf = Seq("2d4383b302998bcbf4c5758143e5c6aa02868402f56ede9ced43838ddfbf1ab1").toDF("complexradical")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("hash with a fixed secret string using a 1 7 substring") {
    AnoToolkit.RANDOM_SECRET = "issamissam123"
    val df = Seq("8488376211007600").toDF("compte")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).hash_substr_1_7("compte")
    val expectedDf = Seq("9bd72ee32500ed2e81368ca74bf053bc936fe8d31a18eef6541434ed18cee023").toDF("compte")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("round to the 10's") {
    val df = Seq(127.6, 122.6).toDF("solde")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).round_ten("solde")
    val expectedDf = Seq(130.0, 120.0).toDF("solde")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("round to the 100's") {
    val df = Seq(2127.6, 6182.6).toDF("solde")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).round_hundred("solde")
    val expectedDf = Seq(2100.0, 6200.0).toDF("solde")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("round to the 1000's") {
    val df = Seq(2127.6, 6982.6).toDF("solde")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).round_thousand("solde")
    val expectedDf = Seq(2000.0, 7000.0).toDF("solde")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("weaken the hour") {
    val df = Seq("2023-12-19 15:30:00").toDF("date")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).weaken_hour("date")
    val expectedDf = Seq("2023-12-19 00:00:00").toDF("date")
      .withColumn("date", col("date").cast("timestamp"))


    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("weaken the day") {
    val df = Seq("2023-12-19 15:30:00").toDF("date")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).weaken_day("date")
    val expectedDf = Seq("2023-12-01 00:00:00").toDF("date")
      .withColumn("date", col("date").cast("timestamp"))
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("weaken the month") {
    val df = Seq("2023-12-19 15:30:00").toDF("date")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).weaken_month("date")
    val expectedDf = Seq("2023-01-01 00:00:00").toDF("date")
      .withColumn("date", col("date").cast("timestamp"))
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("weaken the date") {
    val df = Seq("29102023").toDF("date")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).weaken_date("date")
    val expectedDf = Seq("01102023").toDF("date")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("regex replace") {
    val df = Seq("""""accountNumber"":""1234567890123456"", ""Ville"":""Casablanca""""",
     """""accountNumber"":""6543210987654321""""","""{""Key"":""phoneNumber"",""Value"":""212661687579""}""").toDF("jsonData")
    val result = DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).regexp_mask_r2("jsonData")
    val expectedDf = Seq("""""accountNumber"":""*"", ""Ville"":""Casablanca""""",
      """""accountNumber"":""*""""","""{""Key"":""phoneNumber"",""Value"":""*""}""").toDF("jsonData")
    assertSmallDataFrameEquality(expectedDf, result)
  }

  test("fake firstname") {
    val df =  1.to(1000).map(_.toString).toDF("c1")
    try {
      DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).fake_firstname("c1").show(1)
      DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).fake_lastname("c1").show(1)
      DataFrameMaskingImplicits.DataFrameMaskingImplicits(df).fake_corpname("c1").show(1)
    } catch {
      case _: Exception => throw new IllegalStateException()
    }

  }

}