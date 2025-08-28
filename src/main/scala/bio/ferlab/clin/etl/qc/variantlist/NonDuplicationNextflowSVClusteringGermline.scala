package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationNextflowSVClusteringGermline extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        nextflow_svclustering_germline
          .groupBy($"name").count
          .filter($"count" > 1)
      )
    )
  }
}
