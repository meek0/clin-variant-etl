package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.variantlist.NonDuplicationSNV.run

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
