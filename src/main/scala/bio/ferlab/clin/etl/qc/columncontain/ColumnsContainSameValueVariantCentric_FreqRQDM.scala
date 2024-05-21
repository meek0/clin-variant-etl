package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_FreqRQDM extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.*")
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.affected.*"),
        variant_centric.select($"frequency_RQDM.affected.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.non_affected.*"),
        variant_centric.select($"frequency_RQDM.non_affected.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"frequency_RQDM.total.*"),
        variant_centric.select($"frequency_RQDM.total.*").columns.filterNot(List("an", "pn").contains(_)): _*
      ),
    )
  }
}
