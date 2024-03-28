package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationSNVSomatic extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        snv_somatic
          .groupBy($"chromosome", $"start", $"reference", $"alternate", $"service_request_id", $"bioinfo_analysis_code").count
          .filter($"count" > 1)
      )
    )
  }
}
