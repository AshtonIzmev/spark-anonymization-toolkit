import DataGenerator.{getDb1Table1, getDb2Table1, getDb2Table2}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class AnoToolkitTest extends AnyFunSuite with DataFrameTestToolkit {

  // Add
  // HADOOP_HOME=C:\\hadoop\\
  // to Edit Configurations > Environment Variables
  // And add winutils.exe to C:\\hadoop\\bin\\

  import DataFrameImplicits.DataFrameImplicits

  private val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()

  test("dataframe persisted in orc format can be retrieved") {
    val df1 = DataGenerator.getDb1Table1
    try {
      df1.writeToOrc("src/test/resources/test.orc")
      val df2 = spark.read.orc("src/test/resources/test.orc")
      assertSmallDataFrameEquality(df1, df2)
    }
    finally {
      FileUtils.deleteQuietly(new File("src/test/resources/test.orc/"))
    }
  }

  test("applyMasks should work") {
    val base = "src/test/resources/"
    FileUtils.deleteQuietly(new File(base + "output/"))
    FileUtils.forceMkdir(new File(base + "output/"))

    FileUtils.deleteQuietly(new File("src/test/resources/data/"))
    FileUtils.forceMkdir(new File("src/test/resources/data/"))
    getDb1Table1.writeToOrc("src/test/resources/data/db1_tb1.orc")
    getDb2Table1.writeToOrc("src/test/resources/data/db2_tb1.orc")
    getDb2Table2.writeToOrc("src/test/resources/data/db2_tb2.orc")

    val dataLoader = (db:String, tb:String) => spark.read.orc(base + s"data/${db}_${tb}.orc")

    AnoToolkit.applyMasks(Seq("mask1.json", "mask2.json").map(base+_), base + "output/", dataLoader)

    val df_db1tb1 = spark.read.orc("src/test/resources/output/db1_tb1.orc")
    val df_db2tb1 = spark.read.orc("src/test/resources/output/db2_tb1.orc")
    val df_db2tb2 = spark.read.orc("src/test/resources/output/db2_tb2.orc")
    assert(df_db1tb1.count() == getDb1Table1.count())
    assertSmallDataFrameEquality(df_db2tb1, getDb2Table1.limit(3))
    assert(df_db2tb2.count() == 2)
  }

}
