package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
object NonDuplicationSNV extends TestingApp {
  run { spark =>
    import spark.implicits._
    
    val df = normalized_snv
      .groupBy($"chromosome", $"start", $"reference", $"alternate", $"patient_id").count
      .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }

}
