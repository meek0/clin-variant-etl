package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_FreqRQDMTumor extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select($"freq_rqdm_tumor_only.*"),
        variant_centric.select($"freq_rqdm_tumor_only.*").columns.filterNot(List("pn").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select($"freq_rqdm_tumor_normal.*"),
        variant_centric.select($"freq_rqdm_tumor_normal.*").columns.filterNot(List("pn").contains(_)): _*
      ),
    )
  }
}
