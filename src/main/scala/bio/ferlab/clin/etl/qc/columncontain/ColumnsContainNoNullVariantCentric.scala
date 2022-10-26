package bio.ferlab.clin.etl.qc.columncontain

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object ColumnsContainNoNullVariantCentric extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldNotContainNull(
        variant_centric,
        "chromosome", "start", "reference", "alternate"
      ),
      shouldNotContainNull(
        variant_centric.select(explode($"frequencies_by_analysis") as "frequencies_by_analysis").select($"frequencies_by_analysis.analysis_code" as "analysis_code")
      ),
      shouldNotContainNull(
        variant_centric.select($"frequency_RQDM.*"),
        "affected", "non_affected", "total"
      ),
      shouldNotContainNull(
        variant_centric.select(explode($"donors")).select("col.*"),
        "patient_id", "aliquot_id", "batch_id", "service_request_id", "organization_id"
      ),
    )
  }
}
