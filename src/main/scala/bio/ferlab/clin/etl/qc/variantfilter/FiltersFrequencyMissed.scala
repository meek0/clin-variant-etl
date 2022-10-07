package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
import bio.ferlab.clin.etl.qc.variantfilter.FiltersSNV.run
import org.apache.spark.sql.functions._

object FiltersFrequencyMissed extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNV = normalized_snv
      .filter($"gq" >= 20 && col("filters") === Array("PASS") && $"ad_alt" >= 3 && $"alternate" =!= "*")
      .select("chromosome", "start", "reference", "alternate", "batch_id")
      .dropDuplicates("chromosome", "start", "reference", "alternate")

    val df_NorVar = normalized_variants
      .filter(col("frequency_RQDM").isNull || ($"frequency_RQDM.total.ac" === 0 && $"frequency_RQDM.total.pc" === 0))
      .select("chromosome", "start", "reference", "alternate", "batch_id")

    val df_join = df_NorSNV.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate", "batch_id"), "inner")

    shouldBeEmpty(df_join, "La table devrait etre vide")
  }

}
