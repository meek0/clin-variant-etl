package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object NonDuplicationNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    handleErrors(
      shouldBeEmpty(
        normalized_variants
          .groupBy($"chromosome", $"start", $"reference", $"alternate", $"analysis_id", $"bioinfo_analysis_code").count
          .filter($"count" > 1)
      )
    )
  }
}
