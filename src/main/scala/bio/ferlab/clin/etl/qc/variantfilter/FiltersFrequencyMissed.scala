package bio.ferlab.clin.etl.qc.variantfilter

import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._
import org.apache.spark.sql.functions._

object FiltersFrequencyMissed extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_NorSNV = normalized_snv
      .filter($"gq" >= 20 && col("filters") === Array("PASS") && $"ad_alt" >= 3 && $"alternate" =!= "*")
      .select("chromosome", "start", "reference", "alternate", "analysis_id", "bioinfo_analysis_code")
      .dropDuplicates("chromosome", "start", "reference", "alternate", "analysis_id", "bioinfo_analysis_code")

    val df_NorVar = normalized_variants
      .filter(col("frequency_RQDM").isNull || ($"frequency_RQDM.total.ac" === 0 && $"frequency_RQDM.total.pc" === 0))
      .select("chromosome", "start", "reference", "alternate", "analysis_id", "bioinfo_analysis_code")

    handleErrors(
      shouldBeEmpty(
        df_NorSNV.join(df_NorVar, Seq("chromosome", "start", "reference", "alternate", "analysis_id", "bioinfo_analysis_code"), "inner")
      )
    )
  }
}
