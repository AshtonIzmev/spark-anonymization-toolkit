
import AnoJsonElements._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.io.Source
object PolicyLoader {

  private def mapToObject(jsonStr: String): AnoDatabase = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(jsonStr, classOf[AnoDatabase])
  }

  def loadObject(policyFilePath: String): AnoDatabase = {
    val source:Source = null
    try {
      val source = Source.fromFile(policyFilePath)
      mapToObject(source.mkString)
    }
    finally {
      if (source != null) source.close()
    }
  }


}