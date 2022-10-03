package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

object NonDuplicationVarsome extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df = varsome
      .groupBy($"chromosome", $"start", $"reference", $"alternate").count
      .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }
  
}
