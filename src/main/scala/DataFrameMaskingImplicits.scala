import AnoJsonElements.AnoColumn
import AnoToolkit.RANDOM_SECRET
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object DataFrameMaskingImplicits {

  implicit class DataFrameMaskingImplicits(df: DataFrame) {

    def anonymise(columns: Seq[AnoColumn]): DataFrame = {
      if (Option(columns).isEmpty) return df
      columns.foldLeft(df) { case(df_arg, ac) =>
        ac.policy match {
          case "hash" => df_arg.hash(ac.column)
          case "hash_trim" => df_arg.hash_trim(ac.column)
          case "hash_substr_1_7" => df_arg.hash_substr_1_7(ac.column)
          case "hash_substr_5_7" => df_arg.hash_substr_5_7(ac.column)
          case "hash_substr_7_13" => df_arg.hash_substr_7_13(ac.column)
          case "hash_substr_11_17" => df_arg.hash_substr_11_17(ac.column)
          case "mask" => df_arg.mask(ac.column)

          case "round_ten" => df_arg.round_ten(ac.column)
          case "round_hundred" => df_arg.round_hundred(ac.column)
          case "round_thousand" => df_arg.round_thousand(ac.column)

          case "fake_firstname" => df_arg.fake_firstname(ac.column)
          case "fake_lastname" => df_arg.fake_lastname(ac.column)
          case "fake_corpname" => df_arg.fake_corpname(ac.column)

          case "weaken_hour" => df_arg.weaken_hour(ac.column)
          case "weaken_day" => df_arg.weaken_day(ac.column)
          case "weaken_month" => df_arg.weaken_month(ac.column)
          case "weaken_date" => df_arg.weaken_date(ac.column)

          case "regexp_mask_r2" => df_arg.regexp_mask_r2(ac.column)

          case "keep" => df_arg
          case "drop" => df_arg.drop(ac.column)


          case _ => throw new ClassNotFoundException(s"Anonymisation policy '${ac.policy}' not found")
        }
      }
    }

    def hash(columnName: String): DataFrame = {
      df.withColumn(columnName, sha2(concat(col(columnName), lit(RANDOM_SECRET)), 256))
    }

    def hash_trim(columnName: String): DataFrame = {
      df.withColumn(columnName, sha2(concat(trim(col(columnName)), lit(RANDOM_SECRET)), 256))
    }

    def hash_substr(columnName: String, a:Int, b: Int): DataFrame = {
      df.withColumn(columnName, sha2(concat(substring(col(columnName), a, b), lit(RANDOM_SECRET)), 256))
    }

    def hash_substr_1_7(columnName: String): DataFrame = {
      df.hash_substr(columnName, 1, 7)
    }

    def hash_substr_5_7(columnName: String): DataFrame = {
      df.hash_substr(columnName, 5, 7)
    }

    def hash_substr_7_13(columnName: String): DataFrame = {
      df.hash_substr(columnName, 7, 13)
    }

    def hash_substr_11_17(columnName: String): DataFrame = {
      df.hash_substr(columnName, 11, 17)
    }


    def mask(columnName: String): DataFrame = {
      df.withColumn(columnName, regexp_replace(col(columnName), "[a-zA-Z0-9]", "*"))
    }

    def round_n(columnName: String, n:Int): DataFrame = {
      df.withColumn(columnName, round(col(columnName) / n) * n)
    }

    def round_ten(columnName: String): DataFrame = {
      df.round_n(columnName, 10)
    }

    def round_hundred(columnName: String): DataFrame = {
      df.round_n(columnName, 100)
    }

    def round_thousand(columnName: String): DataFrame = {
      df.round_n(columnName, 1000)
    }

    def fake_firstname(columnName: String): DataFrame = {
      df.withColumn(columnName, AnoToolkit.getFakeFirstname(col(columnName)))
    }

    def fake_lastname(columnName: String): DataFrame = {
      df.withColumn(columnName, AnoToolkit.getFakeLastname(col(columnName)))
    }

    def fake_corpname(columnName: String): DataFrame = {
      df.withColumn(columnName, AnoToolkit.getFakeCorpname(col(columnName)))
    }

    def weaken_hour(columnName: String): DataFrame = {
      df.withColumn(columnName, date_trunc("day",col(columnName)))
    }

    def weaken_day(columnName: String): DataFrame = {
      df.withColumn(columnName, date_trunc("month",col(columnName)))
    }

    def weaken_month(columnName: String): DataFrame = {
      df.withColumn(columnName, date_trunc("year",col(columnName)))
    }

    def weaken_date(columnName: String): DataFrame = {
      df.withColumn(columnName, date_format(trunc(to_date(col(columnName), "ddMMyyyy"), "month"), "ddMMyyyy" ) )
    }

    def multiRegexReplace(columnName: String, r: Seq[(String, String)]): DataFrame = {
      r.foldLeft(df) { case (df_arg,  (r , replacement)) =>
        df_arg.withColumn(columnName, regexp_replace(col(columnName), r, replacement))
      }
    }


    def regexp_mask_r2(columnName: String): DataFrame = {
      val account_number = """accountNumber"":""\d+"""
      val account_number_replacement = """accountNumber"":""*"""

      val phone_number = """Key"":""phoneNumber"",""Value"":""\d+"""
      val phone_number_replacement = """Key"":""phoneNumber"",""Value"":""*"""


      val regexWithReplacements = Seq(
        (account_number, account_number_replacement),
        (phone_number, phone_number_replacement)
      )
      df.multiRegexReplace(columnName,regexWithReplacements)
    }
  }
}