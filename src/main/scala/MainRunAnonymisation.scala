import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object MainRunAnonymisation {

  def main(args: Array[String]): Unit = {
    mainIssam()
  }

  private def mainIssam(): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()

    val baseIssam = "~/data_tmp/data_csv_files/"
    val dataLoader = (db: String, tb: String) =>
      spark.read.option("delimiter", "|").option("header", "true").csv(baseIssam + s"${db}/${tb}.csv")

    FileUtils.deleteQuietly(new File(baseIssam + "zzz/"))
    FileUtils.forceMkdir(new File(baseIssam + "zzz/"))

    val baseMasks = "src/main/resources/"

    AnoToolkit.applyMasks(
      Seq("policy1.json", "policy2.json").map(baseMasks + _),
      baseIssam + "zzz/", dataLoader)
  }

}