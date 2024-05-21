package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Donors extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select("col.*"),
        variant_centric.select(explode($"donors")).select("col.*").columns.filterNot(List("has_alt", "last_update", "variant_type", "sequencing_strategy", "genome_build").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select(explode($"col.hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select(explode($"col.possibly_hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select($"col.exomiser.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"donors")).select($"col.exomiser_other_moi.*")
      ),
    )
  }
}
