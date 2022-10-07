package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
object NonDuplicationVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df = variants
      .groupBy($"chromosome", $"start", $"reference", $"alternate").count
      .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }
  
}
