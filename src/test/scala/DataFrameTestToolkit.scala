import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType


trait DataFrameTestToolkit {

  case class DataFrameSchemaMismatch(smth: String) extends Exception(smth)
  case class DataFrameContentMismatch(smth: String) extends Exception(smth)

  private def schemaMismatchMessage(actualDS: DataFrame, expectedDS: DataFrame): String = {
    s"""
  Actual Schema:
  ${actualDS.schema}
  Expected Schema:
  ${expectedDS.schema}
  """
  }

  private def contentMismatchMessage(actual: DataFrame, expected: DataFrame): String = {
    actual.collect().zip(expected.collect())
      .map(rows => rows._1.toString() + " __ " + rows._2.toString())
      .mkString("\n")
  }

  private def schemaEquality(actualSchema: StructType, expectedSchema: StructType): Boolean = {
    actualSchema.fields.sortBy(_.name).zip(expectedSchema.fields.sortBy(_.name))
      .foldLeft(true) { (b, e) => b && (e._1.name == e._2.name) && (e._1.dataType == e._2.dataType) }
  }

  def assertSmallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Unit = {
    if (!schemaEquality(actualDF.schema, expectedDF.schema)) {
      throw DataFrameSchemaMismatch(schemaMismatchMessage(actualDF, expectedDF))
    }
    if (!actualDF.collect().sameElements(expectedDF.collect())) {
      throw DataFrameContentMismatch(contentMismatchMessage(actualDF, expectedDF))
    }
  }

}