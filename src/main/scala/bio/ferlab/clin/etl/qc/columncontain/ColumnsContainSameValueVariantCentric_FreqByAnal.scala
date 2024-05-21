package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_FreqByAnal extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.affected.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.non_affected.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"frequencies_by_analysis")).select("col.total.*")
      ),
    )
  }
}
