package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

// Test:               Les variants doivent être unique par batch_id
// Objectifs du test:  Vérifier que les variants ne sont pas dupliqués dans la table normalized_variants
// Critère de succès:  La table doit être vide

object NonDuplicationNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df = normalized_variants.groupBy($"chromosome", $"start", $"reference", $"alternate", $"batch_id").count
    .filter($"count" > 1)

    shouldBeEmpty(df, "La table devrait etre vide")
  }
}
