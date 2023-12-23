import com.fasterxml.jackson.annotation.JsonProperty

object AnoJsonElements {

  case class AnoColumn(
                        @JsonProperty("column") column: String,
                        @JsonProperty("classification") classification: String,
                        @JsonProperty("policy") policy: String
                      )

  case class AnoTable(
                       @JsonProperty("table") table: String,
                       @JsonProperty("limit") limit: Int,
                       @JsonProperty("columns") columns: Seq[AnoColumn]
                     )

  case class AnoDatabase(
                          @JsonProperty("db") database: String,
                          @JsonProperty("tables") tables: Seq[AnoTable]
                        )
}