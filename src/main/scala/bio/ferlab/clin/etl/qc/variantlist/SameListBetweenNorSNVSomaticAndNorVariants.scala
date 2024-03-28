package bio.ferlab.clin.etl.qc.variantlist

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object SameListBetweenNorSNVSomaticAndNorVariants extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNVSomatic = normalized_snv_somatic
      .filter($"ad_alt" >= 3)
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_NorVar = normalized_variants
      .select($"chromosome", $"start", $"reference", $"alternate")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    handleErrors(
      shouldBeEmpty(
        df_NorSNVSomatic.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate"), "left_anti")
      )
    )
  }
}
