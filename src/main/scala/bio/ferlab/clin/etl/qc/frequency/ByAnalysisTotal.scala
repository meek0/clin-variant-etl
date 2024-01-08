package bio.ferlab.clin.etl.qc.frequency

import org.apache.spark.sql.functions.{col, explode, count}
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ByAnalysisTotal extends TestingApp {
  run { spark =>
    import spark.implicits._

    val NbPatients = normalized_snv
    .groupBy($"patient_id").count
    .count

    val NbAnalysisCode = normalized_snv
    .groupBy("analysis_code", "patient_id").count
    .groupBy("analysis_code").agg(count("*").as("expected_pn"))

    val df_expected_Freq = normalized_snv
    .select($"chromosome", $"start", $"reference", $"alternate", $"patient_id", $"ad_alt", $"gq", $"filters", $"calls", $"analysis_code", $"affected_status_code")
    .dropDuplicates
    .groupBy($"chromosome", $"start", $"reference", $"alternate", $"analysis_code")
    .agg(ac, pc)
    .join(NbAnalysisCode, "analysis_code")
    .withColumn("expected_an", col("expected_pn") * 2)

    handleErrors(
      shouldBeEmpty(
        variant_centric
        .select($"chromosome", $"start", $"reference", $"alternate", explode($"frequencies_by_analysis"))
        .select($"chromosome", $"start", $"reference", $"alternate", $"col.analysis_code", $"col.total.*")
        .join(df_expected_Freq, Seq("chromosome", "start", "reference", "alternate", "analysis_code"), "inner")
        .filter(!($"expected_ac" <=> $"ac") || !($"expected_an" <=> $"an") || !($"expected_pc" <=> $"pc") || !($"expected_pn" <=> $"pn"))
      )
    )
  }
}
