package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationCoverageByGene extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        coverage_by_gene
          .groupBy($"gene", $"aliquot_id", $"batch_id").count
          .filter($"count" > 1)
      )
    )
  }
}
