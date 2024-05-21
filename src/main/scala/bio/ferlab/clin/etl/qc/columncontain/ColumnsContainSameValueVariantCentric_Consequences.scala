package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Consequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variant_centric.select(explode($"consequences")).select("col.*"),
        variant_centric.select(explode($"consequences")).select("col.*").columns.filterNot(List("feature_type", "picked").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variant_centric.select(explode($"consequences")).select("col.predictions.*")
      ),
    )
  }
}
