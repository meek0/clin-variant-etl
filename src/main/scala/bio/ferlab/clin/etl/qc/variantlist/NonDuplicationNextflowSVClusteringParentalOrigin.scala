package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationNextflowSVClusteringParentalOrigin extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        nextflow_svclustering_parental_origin
          .groupBy($"name", $"service_request_id").count
          .filter($"count" > 1)
      )
    )
  }
}
