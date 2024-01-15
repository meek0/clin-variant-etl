package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.model._
import bio.ferlab.clin.model.enriched.{EXOMISER, EXOMISER_OTHER_MOI, EnrichedSNVSomaticTumorOnly}
import bio.ferlab.clin.model.normalized.{NormalizedExomiser, NormalizedSNVSomaticTumorOnly}
import bio.ferlab.clin.testutils.WithTestConfig
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.testutils.{SparkSpec, DeprecatedTestETLContext}

class SNVSomaticTumorOnlySpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val normalized_snv: DatasetConf = conf.getDataset("normalized_snv_somatic_tumor_only")
  val normalized_exomiser: DatasetConf = conf.getDataset("normalized_exomiser")

  val job = SNVSomaticTumorOnly(DeprecatedTestETLContext())

  it should "enrich data with exomiser" in {
    val snvDf = Seq(
      NormalizedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1"),
      NormalizedSNVSomaticTumorOnly(chromosome = "1", start = 2, reference = "A", alternate = "C", aliquot_id = "aliquot1"),
      NormalizedSNVSomaticTumorOnly(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1"),

      NormalizedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2"), // no exomiser data

      NormalizedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3"),
    ).toDF()

    val exomiserDf = Seq(
      // aliquot1
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 1), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", contributing_variant = true, moi = "AD", gene_combined_score = 0.99f), // should be in exomiser_other_moi

      NormalizedExomiser(chromosome = "1", start = 2, reference = "A", alternate = "C", aliquot_id = "aliquot1", exomiser_variant_score = 1, contributing_variant = false), // exomiser_variant_score only

      NormalizedExomiser(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = true, moi = "XR", gene_combined_score = 0.99f), // should be in exomiser
      NormalizedExomiser(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1", contributing_variant = false, gene_combined_score = 1), // should not be in exomiser_other_moi

      // aliquot3
      NormalizedExomiser(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3", contributing_variant = true, moi = "XR", gene_combined_score = 0.5f), // should be in exomiser
    ).toDF()

    val data = Map(
      normalized_snv.id -> snvDf,
      normalized_exomiser.id -> exomiserDf
    )
    val result = job.transformSingle(data)

    result
      .as[EnrichedSNVSomaticTumorOnly]
      .collect() should contain theSameElementsAs Seq(
      EnrichedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot1", exomiser = Some(EXOMISER(moi = "XR", gene_combined_score = 1)), exomiser_other_moi = Some(EXOMISER_OTHER_MOI(moi = "AD", gene_combined_score = 0.99f))),
      EnrichedSNVSomaticTumorOnly(chromosome = "1", start = 2, reference = "A", alternate = "C", aliquot_id = "aliquot1", exomiser = None, exomiser_other_moi = None),
      EnrichedSNVSomaticTumorOnly(chromosome = "1", start = 3, reference = "C", alternate = "G", aliquot_id = "aliquot1", exomiser = Some(EXOMISER(moi = "XR", gene_combined_score = 0.99f)), exomiser_other_moi = None),
      EnrichedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot2", exomiser = None, exomiser_other_moi = None),
      EnrichedSNVSomaticTumorOnly(chromosome = "1", start = 1, reference = "T", alternate = "A", aliquot_id = "aliquot3", exomiser = Some(EXOMISER(moi = "XR", gene_combined_score = 0.5f)), exomiser_other_moi = None),
    )
  }
}

