package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import bio.ferlab.clin.etl.qc.columncontain.ColumnsContainOnlyNullVariantCentric_Donors.variant_centric
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Donors extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        donors.select("col.*"),
        donors.select("col.*").columns.filterNot(List("has_alt", "last_update", "variant_type", "sequencing_strategy", "genome_build").contains(_)): _*
      ),
      shouldNotContainSameValue(
        donors.select(explode($"col.hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        donors.select(explode($"col.possibly_hc_complement")).select("col.*")
      ),
      shouldNotContainSameValue(
        donors.select($"col.exomiser.*")
      ),
      shouldNotContainSameValue(
        donors.select($"col.exomiser_other_moi.*")
      ),
    )
  }
}
