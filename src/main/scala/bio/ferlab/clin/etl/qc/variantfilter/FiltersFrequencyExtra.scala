package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp.shouldBeEmpty
import bio.ferlab.clin.etl.qc.variantfilter.FiltersSNV.run
import org.apache.spark.sql.functions._

object FiltersFrequencyExtra extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNV = normalized_snv
      .join(normalized_snv.filter($"gq" >= 20 && col("filters") === Array("PASS") && $"ad_alt" >= 3 && $"alternate" =!= "*"), Seq("chromosome", "start", "reference", "alternate"), "left_anti")
      .select("chromosome", "start", "reference", "alternate")

    val df_NorVar = normalized_variants
      .filter($"frequency_RQDM.total.ac" =!= 0 || $"frequency_RQDM.total.pc" =!= 0)
      .select("chromosome", "start", "reference", "alternate")

    val df_join = df_NorSNV.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate"), "inner")

    shouldBeEmpty(df_join, "La table devrait etre vide")
  }

}
