package bio.ferlab.clin.etl.qc.frequency

import org.apache.spark.sql.functions.{col, explode, count}
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object ByAnalysisAffected extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_normalized_snv_filter = normalized_snv
    .where($"affected_status_code" === "affected")

    val NbPatients = df_normalized_snv_filter
    .groupBy($"aliquot_id").count
    .count

    val NbAnalysisCode = df_normalized_snv_filter
    .groupBy("analysis_code", "aliquot_id").count
    .groupBy("analysis_code").agg(count("*").as("expected_pn"))

    val df_expected_Freq = df_normalized_snv_filter
    .select($"chromosome", $"start", $"reference", $"alternate", $"aliquot_id", $"ad_alt", $"gq", $"filters", $"calls", $"analysis_code", $"affected_status_code", $"aliquot_id")
    .dropDuplicates
    .groupBy($"chromosome", $"start", $"reference", $"alternate", $"analysis_code")
    .agg(ac, pc)
    .join(NbAnalysisCode, "analysis_code")
    .withColumn("expected_an", col("expected_pn") * 2)

    handleErrors(
      shouldBeEmpty(
        variant_centric
        .select($"chromosome", $"start", $"reference", $"alternate", explode($"frequencies_by_analysis"))
        .select($"chromosome", $"start", $"reference", $"alternate", $"col.analysis_code", $"col.affected.*")
        .join(df_expected_Freq, Seq("chromosome", "start", "reference", "alternate", "analysis_code"), "inner")
        .filter(!($"expected_ac" <=> $"ac") || !($"expected_an" <=> $"an") || !($"expected_pc" <=> $"pc") || !($"expected_pn" <=> $"pn"))
      )
    )
  }
}
