package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
object SameListBetweenSNVAndNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNV = normalized_snv
      .filter($"ad_alt" >= 3)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_NorVar = normalized_variants
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_Diff = df_NorSNV.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate"), "left_anti")

    shouldBeEmpty(df_Diff, "La table devrait etre vide")
  }

}
