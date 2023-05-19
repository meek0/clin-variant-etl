package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationNorSNV extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        normalized_snv
          .groupBy($"chromosome", $"start", $"reference", $"alternate", $"service_request_id").count
          .filter($"count" > 1)
      )
    )
  }
}
