package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_FreqByAnal extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*"),
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*").columns.filterNot(List("analysis_display_name"/*CLIN-1358*/).contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.non_affected.*")
      ),
      shouldNotContainOnlyNull(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.total.*")
      ),
    )
  }
}
