import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import java.sql.Date

object DataGenerator {

  private val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  def getFakeData: DataFrame = {
    Seq(
      ("ziad", "aboubakr", "ziad@gmail.com", "2000-05-08", "1234234534564567", "680092094", "2 av Jamaa Badr RABAT"),
      ("hakim", "haimer", "hakim@gmail.com", "2000-04-06", "9876876576546543", "680094322", "47 bd des FAR CASA")
    ).toDF("firstname", "lastname", "email", "birth_date", "credit_card_number", "phone_number", "address")
  }

  def getDb1Table1: DataFrame = {
    Seq(
      ("ziad", 154123, Date.valueOf("2020-01-21")),
      ("hakim", 927391, Date.valueOf("1997-03-07"))
    ).toDF("col1", "col2", "col3")
  }

  def getDb2Table1: DataFrame =
    Seq("ziad", "hakim", "ali", "hamza", "aicha", "hicham", "hamid", "fatima").toDF("col1")

  def getDb2Table2: DataFrame = {
    Seq(
      ("AB1234", "ziad", 154123),
      ("C123", "hakim", 927391)
    ).toDF("col1", "col2", "col3")
  }

}