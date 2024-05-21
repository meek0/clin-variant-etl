package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_Donors extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select("col.*"),
        variant_centric.select(explode($"donors")).select("col.*").columns.filterNot(List("analysis_display_name"/*CLIN-1358*/, "practitioner_role_id").contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select(explode($"col.hc_complement")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select(explode($"col.possibly_hc_complement")).select("col.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select($"col.exomiser.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"donors")).select($"col.exomiser_other_moi.*")
      ),
    )
  }
}
