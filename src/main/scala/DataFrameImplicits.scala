
import org.apache.spark.sql.DataFrame

object DataFrameImplicits {

  implicit class DataFrameImplicits(df: DataFrame) {

    def limitOrAll(n: Int): DataFrame = if (n == -1) df else df.limit(n)

    def writeToOrc(path: String): Unit = df.write.orc(path)

  }
}