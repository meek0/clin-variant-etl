package bio.ferlab.clin.etl.qc.frequency

import org.apache.spark.sql.functions.lit
import bio.ferlab.clin.etl.qc.TestingApp
import bio.ferlab.clin.etl.qc.TestingApp._

object RQDMTumorNormal extends TestingApp {
  run { spark =>
    import spark.implicits._

    val df_normalized_snv_somatic_filter = normalized_snv_somatic
    .where($"bioinfo_analysis_code" === "TNEBA")

    val NbPatients = df_normalized_snv_somatic_filter
    .groupBy($"sample_id").count
    .count

    val df_expected_Freq = df_normalized_snv_somatic_filter
    .select($"chromosome", $"start", $"reference", $"alternate", $"patient_id", $"ad_alt", $"sq", $"filters", $"calls", $"analysis_code", $"affected_status_code")
    .groupBy($"chromosome", $"start", $"reference", $"alternate")
    .agg(pc_somatic)
    .withColumn("expected_pn", lit(NbPatients))

    handleErrors(
      shouldBeEmpty(
        variant_centric
        .select($"chromosome", $"start", $"reference", $"alternate", $"freq_rqdm_tumor_normal.*")
        .join(df_expected_Freq, Seq("chromosome", "start", "reference", "alternate"), "inner")
        .filter(!($"expected_pc" <=> $"pc") || !($"expected_pn" <=> $"pn"))
      )
    )
  }
}
