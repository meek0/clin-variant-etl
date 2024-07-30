package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainOnlyNullVariantCentric_FreqByAnal extends TestingApp {
  run { spark =>
    import spark.implicits._

    val freqs = variant_centric.select(explode($"frequencies_by_analysis"))
    val freqsCols = freqs.select("col.*")

    handleErrors(
      shouldNotContainOnlyNull(
        freqsCols,
        freqsCols.columns.filterNot(List("analysis_display_name"/*CLIN-1358*/).contains(_)): _*
      ),
      shouldNotContainOnlyNull(
        freqs.select("col.affected.*")
      ),
      shouldNotContainOnlyNull(
        freqs.select("col.non_affected.*")
      ),
      shouldNotContainOnlyNull(
        freqs.select("col.total.*")
      ),
    )
  }
}
