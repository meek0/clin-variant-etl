package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainSameValueVariantCentric_Consequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainSameValue(
        variants_consequences.select("col.*"),
        variants_consequences.select("col.*").columns.filterNot(List("feature_type", "picked").contains(_)): _*
      ),
      shouldNotContainSameValue(
        variants_consequences.select("col.predictions.*")
      ),
    )
  }
}
