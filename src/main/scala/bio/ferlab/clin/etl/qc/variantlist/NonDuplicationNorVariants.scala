package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

object NonDuplicationNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df = normalized_variants
      .groupBy($"chromosome", $"start", $"reference", $"alternate", $"batch_id").count
      .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }
  
}
