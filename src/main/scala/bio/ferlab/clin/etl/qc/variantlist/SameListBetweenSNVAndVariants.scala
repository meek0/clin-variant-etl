package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

// Test:               Validation de la liste des variants entre la table normalized_snv et la table variants
// Objectifs du test:  Vérifier que la liste des variants ayant ad_alt >= 3 dans la table normalized_snv est incluse dans celle de la table variants
// Critère de succès:  La table doit être vide

object SameListBetweenSNVAndVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNV = normalized_snv
      .filter($"ad_alt" >= 3)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Var = variants
      .dropDuplicates("chromosome", "start", "reference", "alternate")
      .select($"chromosome", $"start", $"reference", $"alternate")

    val df_Diff = df_NorSNV.join(df_Var, Seq("chromosome", "start", "reference", "alternate"), "left_anti")

    shouldBeEmpty(df_Diff, "La table devrait etre vide")
  }

}
