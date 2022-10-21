package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationVarsome extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        varsome
          .groupBy($"chromosome", $"start", $"reference", $"alternate").count
          .filter($"count" > 1)
      )
    )
  }
}
