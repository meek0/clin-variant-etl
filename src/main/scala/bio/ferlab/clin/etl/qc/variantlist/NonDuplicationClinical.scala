package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.{handleErrors, shouldBeEmpty}

object NonDuplicationClinical extends TestingApp {
  run { spark =>
   import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        clinical
          .groupBy($"analysis_service_request_id", $"service_request_id", $"bioinfo_analysis_code").count
          .filter($"count" > 1)
      )
    )
  }
}
