package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationNorConsequences extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        normalized_consequences
          .groupBy($"chromosome", $"start", $"reference", $"alternate", $"ensembl_transcript_id").count
          .filter($"count" > 1)
      )
    )
  }
}
