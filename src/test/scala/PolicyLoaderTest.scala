import org.scalatest.funsuite.AnyFunSuite

class PolicyLoaderTest extends AnyFunSuite {

  test("mask1 should be correctly loaded and cast into JsonElements case classes") {
    val obj = PolicyLoader.loadObject("src/test/resources/mask1.json")
    assertResult("db1")(obj.database)
    assertResult(1)(obj.tables.length)
    assertResult("tb1")(obj.tables.head.table)
    assertResult(-1)(obj.tables.head.limit)
    assertResult(3)(obj.tables.head.columns.length)
    assertResult("col1")(obj.tables.head.columns.head.column)
    assertResult("personnelle")(obj.tables.head.columns.head.classification)
    assertResult("fake_firstname")(obj.tables.head.columns.head.policy)
  }

  test("mask2 should be correctly loaded and cast into JsonElements case classes") {
    val obj = PolicyLoader.loadObject("src/test/resources/mask2.json")
    assertResult("db2")(obj.database)
    assertResult(2)(obj.tables.length)
    assertResult("tb1")(obj.tables.head.table)
    assertResult(3)(obj.tables.head.limit)
    assertResult(2)(obj.tables.last.limit)
    assertResult(null)(obj.tables.head.columns)
    assertResult(1)(obj.tables.last.columns.length)
  }

}
